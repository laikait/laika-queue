<?php

namespace Laika\Queue;

use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Interfaces\FailedJobProviderInterface;
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

    public function work(string $queue = 'default', int $sleep = 3, int $timeout = 60, int $memoryLimit = 128): void
    {
        while (!$this->shouldQuit) {
            if ($this->paused) {
                usleep(500000);
                continue;
            }

            $job = $this->driver->pop($queue);

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
                $this->driver->release($job->id, $queue, $job->backoff());
                break;
            }
            usleep(100000);
        }
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
}
