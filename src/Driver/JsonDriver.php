<?php

declare(strict_types=1);

namespace Laika\Queue\Driver;

use Laika\Queue\Abstracts\Job;
use Laika\Core\Storage\JsonStorage;
use Laika\Queue\Interfaces\QueueDriverInterface;

/**
 * Flat-file queue driver — good for local/dev, no external service needed.
 * Every queue shares a single lf-storage/queues/jobs.json file (one JSON
 * array of job records, each tagged with its 'queue'). Reads and writes go
 * through JsonStorage::mutate(), which holds one exclusive lock across the
 * whole read-modify-write so concurrent push()/pop() calls can't corrupt the
 * file or hand the same job to two workers. Same locking-clause caveat as
 * DatabaseDriver applies — see README — a job whose worker dies stays
 * reserved, so jobs should still be idempotent.
 */
class JsonDriver implements QueueDriverInterface
{
    protected JsonStorage $store;

    public function __construct()
    {
        $this->store = new JsonStorage(APP_PATH . '/lf-storage/queues');
    }

    public function install(): void {}

    /**
     * Run $fn against jobs.json under one exclusive lock.
     * The callback returns ['records' => …] to write, 'return' => … to hand back.
     * Omitting 'records' skips the write entirely.
     */
    protected function withLock(callable $fn): mixed
    {
        return $this->store->mutate('jobs', $fn);
    }

    /**
     * Push Job
     * @param Job $job Job to push
     * @param string $queue Queue
     * @param int $delay Queue delay. Defaul is 0.
     * @return string
     */
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

    /**
     * Pop queue jobs
     * @param string $queue Queue
     * @return ?Job
     */
    public function pop(string $queue = 'default'): ?Job
    {
        return $this->withLock(function (array $records) use ($queue) {
            $now = time();
            foreach ($records as &$r) {
                if ($r['queue'] === $queue && $r['reserved_at'] === null && $r['available_at'] <= $now) {
                    $r['reserved_at'] = $now;
                    $r['attempts'] += 1;

                    $job = Job::unserializePayload($r['payload']);
                    $job->id = $r['id'];
                    $job->queue = $queue;
                    $job->tries = $r['attempts'];

                    unset($r);
                    return ['records' => $records, 'return' => $job];
                }
            }
            unset($r);

            // Nothing claimed — no write
            return ['return' => null];
        });
    }

    /**
     * Delete a single queue
     * @param string $id Queue job ID
     * @param string $queue Queue. Default is 'default'
     * @return void
     */
    public function ack(string $id, string $queue = 'default'): void
    {
        $this->delete($id, $queue);
    }

    /**
     * Release a single queue
     * @param string $id Queue job ID
     * @param string $queue Queue. Default is 'default'
     * @param int $delay Queue delay. Default is 0.
     * @return void
     */
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
            unset($r);
            return ['records' => $records];
        });
    }

    /**
     * Delete a single queue
     * @param string $id Queue job ID
     * @param string $queue Queue. Default is 'default'
     * @return void
     */
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

    /**
     * Size of Queue
     * @param string $queue Queue. Default is 'default'
     * @return int
     */
    public function size(string $queue = 'default'): int
    {
        return $this->withLock(function (array $records) use ($queue) {
            $count = count(array_filter(
                $records,
                fn($r) => $r['queue'] === $queue && $r['reserved_at'] === null
            ));

            // Read only — no write
            return ['return' => $count];
        });
    }
}
