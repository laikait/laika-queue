<?php

namespace Laika\Queue;

use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Interfaces\FailedJobProviderInterface;
use Laika\Queue\Interfaces\ReconnectableDriver;
use Laika\Queue\Abstracts\Job;

class Worker
{
    protected QueueDriverInterface $driver;
    protected ?FailedJobProviderInterface $failer;
    protected bool $shouldQuit = false;
    protected bool $paused = false;

    public function __construct(QueueDriverInterface $driver, ?FailedJobProviderInterface $failer = null)
    {
        $this->driver = $driver;
        $this->failer = $failer;
        $this->listenForSignals();
    }

    protected function listenForSignals(): void
    {
        if (!extension_loaded('pcntl')) {
            return;
        }

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn() => $this->shouldQuit = true);
        pcntl_signal(SIGINT, fn() => $this->shouldQuit = true);
        pcntl_signal(SIGUSR2, fn() => $this->paused = true);
        pcntl_signal(SIGCONT, fn() => $this->paused = false);
    }

    public function work(string $queue = 'default', int $sleep = 3, int $timeout = 60, ?int $memoryLimit = null): void
    {
        $memoryLimit ??= $this->resolveMemoryLimitMb();

        while (!$this->shouldQuit) {
            if ($this->paused) {
                usleep(500000);
                continue;
            }

            try {
                $job = $this->driver->pop($queue);
            } catch (\Throwable $e) {
                fwrite(STDERR, "[laika-queue] driver->pop() failed: {$e->getMessage()}\n");
                sleep($sleep);
                $this->stopIfMemoryExceeded($memoryLimit);
                continue;
            }

            if (!$job) {
                sleep($sleep);
                $this->stopIfMemoryExceeded($memoryLimit);
                continue;
            }

            $this->runJob($job, $queue, $timeout);
            $this->stopIfMemoryExceeded($memoryLimit);
        }
    }

    protected function runJob(Job $job, string $queue, int $timeout): void
    {
        if (!extension_loaded('pcntl')) {
            $this->process($job, $queue);
            return;
        }

        $pid = pcntl_fork();

        if ($pid === 0) {
            if ($this->driver instanceof ReconnectableDriver) {
                $this->driver->reconnect();
            }
            if ($this->failer instanceof ReconnectableDriver) {
                $this->failer->reconnect();
            }
            $this->process($job, $queue);
            exit(0);
        }

        $start = time();
        while (true) {
            $status = null;
            if (pcntl_waitpid($pid, $status, WNOHANG) === $pid) {
                break;
            }
            if (time() - $start >= $timeout) {
                posix_kill($pid, SIGKILL);
                pcntl_waitpid($pid, $status);
                $this->handleTimeout($job, $queue);
                break;
            }
            usleep(100000);
        }
    }

    protected function handleTimeout(Job $job, string $queue): void
    {
        if ($job->tries >= $job->maxTries) {
            $this->failer?->log($queue, $job->serializePayload(), new \RuntimeException(
                "Job '{$job->id}' timed out and exhausted its {$job->maxTries} tries."
            ));
            $this->driver->delete($job->id, $queue);
            return;
        }

        $this->driver->release($job->id, $queue, $job->backoff());
    }

    protected function process(Job $job, string $queue): void
    {
        try {
            $job->handle();
            $this->driver->ack($job->id, $queue);
        } catch (\Throwable $e) {
            $job->failed($e);

            if ($job->tries >= $job->maxTries) {
                $this->failer?->log($queue, $job->serializePayload(), $e);
                $this->driver->delete($job->id, $queue);
            } else {
                $this->driver->release($job->id, $queue, $job->backoff());
            }
        }
    }

    protected function stopIfMemoryExceeded(int $limitMb): void
    {
        if ((memory_get_usage(true) / 1024 / 1024) >= $limitMb) {
            $this->shouldQuit = true;
        }
    }

    /**
     * Auto-derive the soft memory-limit threshold (MB) used by
     * stopIfMemoryExceeded() when work() isn't given an explicit one —
     * ~90% of PHP's actual current memory_limit ini setting (as read via
     * Laika\Core\System\MemoryManager, when installed — see
     * laikait/laika-core), so the worker exits gracefully (for
     * supervisor/systemd to restart) before genuinely risking a hard OOM
     * mid-job, rather than a fixed number picked without knowing the real
     * ceiling.
     *
     * Falls back to a flat 128MB when laika-core isn't installed, the ini
     * value can't be parsed, or memory_limit is unlimited ('-1') — this
     * class otherwise has no hard dependency on laika-core, same
     * lazy-reference pattern as DatabaseDriver::ensureSchema().
     */
    protected function resolveMemoryLimitMb(): int
    {
        $fallback = 128;

        if (!class_exists(\Laika\Core\System\MemoryManager::class)) {
            return $fallback;
        }

        $limit = trim((string) (new \Laika\Core\System\MemoryManager())->currentLimit());

        if (!preg_match('/^(\d+)\s*([kmg])$/i', $limit, $m)) {
            return $fallback;
        }

        $mb = match (strtolower($m[2])) {
            'g' => (int) $m[1] * 1024,
            'k' => (int) ceil((int) $m[1] / 1024),
            default => (int) $m[1], // 'm'
        };

        return max(1, (int) floor($mb * 0.9));
    }
}
