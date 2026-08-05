<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Abstracts\Job;

class JsonDriver implements QueueDriverInterface
{
    protected string $dir;

    public function __construct(string $dir)
    {
        $this->dir = rtrim($dir, '/');
        if (!is_dir($this->dir)) {
            mkdir($this->dir, 0775, true);
        }
    }

    protected function path(string $queue): string
    {
        return "{$this->dir}/{$queue}.json";
    }

    protected function withLock(string $queue, callable $fn): mixed
    {
        $file = $this->path($queue);
        $handle = fopen($file, 'c+');
        flock($handle, LOCK_EX);

        $raw = stream_get_contents($handle);
        $records = $raw ? json_decode($raw, true) : [];

        $result = $fn($records);

        ftruncate($handle, 0);
        rewind($handle);
        fwrite($handle, json_encode($result['records'] ?? $records, JSON_PRETTY_PRINT));
        fflush($handle);
        flock($handle, LOCK_UN);
        fclose($handle);

        return $result['return'] ?? null;
    }

    public function push(Job $job, string $queue = 'default', int $delay = 0): string
    {
        $job->id = $job->id ?: bin2hex(random_bytes(16));
        $job->queue = $queue;
        $now = time();

        return $this->withLock($queue, function (array $records) use ($job, $delay, $now) {
            $records[] = [
                'id' => $job->id,
                'payload' => $job->serializePayload(),
                'attempts' => 0,
                'reserved_at' => null,
                'available_at' => $now + $delay,
                'created_at' => $now,
            ];
            return ['records' => $records, 'return' => $job->id];
        });
    }

    public function pop(string $queue = 'default'): ?Job
    {
        $now = time();

        return $this->withLock($queue, function (array $records) use ($queue, $now) {
            foreach ($records as $i => &$r) {
                if ($r['reserved_at'] === null && $r['available_at'] <= $now) {
                    $r['reserved_at'] = $now;
                    $r['attempts'] += 1;

                    $job = Job::unserializePayload($r['payload']);
                    $job->id = $r['id'];
                    $job->queue = $queue;
                    $job->tries = $r['attempts'];

                    return ['records' => $records, 'return' => $job];
                }
            }
            return ['records' => $records, 'return' => null];
        });
    }

    public function ack(string $id, string $queue = 'default'): void
    {
        $this->delete($id, $queue);
    }

    public function release(string $id, string $queue = 'default', int $delay = 0): void
    {
        $this->withLock($queue, function (array $records) use ($id, $delay) {
            foreach ($records as &$r) {
                if ($r['id'] === $id) {
                    $r['reserved_at'] = null;
                    $r['available_at'] = time() + $delay;
                    break;
                }
            }
            return ['records' => $records];
        });
    }

    public function delete(string $id, string $queue = 'default'): void
    {
        $this->withLock($queue, function (array $records) use ($id) {
            $records = array_values(array_filter($records, fn($r) => $r['id'] !== $id));
            return ['records' => $records];
        });
    }

    public function size(string $queue = 'default'): int
    {
        return $this->withLock($queue, function (array $records) {
            $count = count(array_filter($records, fn($r) => $r['reserved_at'] === null));
            return ['records' => $records, 'return' => $count];
        });
    }
}
