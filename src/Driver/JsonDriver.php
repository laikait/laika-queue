<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Abstracts\Job;

/**
 * Flat-file queue driver — good for local/dev, no external service needed.
 * Every queue shares a single lf-storage/queues/jobs.json file (one JSON
 * array of job records, each tagged with its 'queue'), guarded by flock()
 * so concurrent push()/pop() calls don't corrupt it. Same locking-clause
 * caveat as DatabaseDriver applies — see README — this isn't safe against
 * multiple worker processes racing on the same queue without idempotent
 * jobs.
 */
class JsonDriver implements QueueDriverInterface
{
    protected string $file;

    public function __construct()
    {
        $this->file = APP_PATH . '/lf-storage/queues/jobs.json';

        $dir = dirname($this->file);
        if (!is_dir($dir)) {
            mkdir($dir, recursive:true);
            setPermission($dir, 0775);
        }
        if (!is_file($this->file)) {
            file_put_contents($this->file, json_encode([]));
        }
    }

    protected function withLock(callable $fn): mixed
    {
        $handle = fopen($this->file, 'c+');
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

        return $this->withLock(function (array $records) use ($job, $queue, $delay, $now) {
            $records[] = [
                'id' => $job->id,
                'queue' => $queue,
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

        return $this->withLock(function (array $records) use ($queue, $now) {
            foreach ($records as $i => &$r) {
                if ($r['queue'] === $queue && $r['reserved_at'] === null && $r['available_at'] <= $now) {
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
        $this->withLock(function (array $records) use ($id, $queue, $delay) {
            foreach ($records as &$r) {
                if ($r['id'] === $id && $r['queue'] === $queue) {
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
        $this->withLock(function (array $records) use ($id, $queue) {
            $records = array_values(array_filter(
                $records,
                fn($r) => !($r['id'] === $id && $r['queue'] === $queue)
            ));
            return ['records' => $records];
        });
    }

    public function size(string $queue = 'default'): int
    {
        return $this->withLock(function (array $records) use ($queue) {
            $count = count(array_filter(
                $records,
                fn($r) => $r['queue'] === $queue && $r['reserved_at'] === null
            ));
            return ['records' => $records, 'return' => $count];
        });
    }
}
