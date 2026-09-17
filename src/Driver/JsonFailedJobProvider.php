<?php

declare(strict_types=1);

namespace Laika\Queue\Driver;

use Laika\Core\Storage\JsonStorage;
use Laika\Queue\Interfaces\FailedJobProviderInterface;

class JsonFailedJobProvider implements FailedJobProviderInterface
{
    protected JsonStorage $store;

    public function __construct()
    {
        $this->store = new JsonStorage(APP_PATH . '/lf-storage/queues');
    }

    public function install(): void {}

    /**
     * Log Failed queue
     * @param string $queue Queue
     * @param string $payload Log payload
     * @param string $payload Log payload
     * @param Throwable $e Throwable object
     */
    public function log(string $queue, string $payload, \Throwable $e): string
    {
        $id = bin2hex(random_bytes(16));

        return $this->withLock(function (array $records) use ($id, $queue, $payload, $e) {
            $records[] = [
                'id' => $id,
                'queue' => $queue,
                'payload' => $payload,
                'exception' => (string) $e,
                'failed_at' => time(),
            ];
            return ['records' => $records, 'return' => $id];
        });
    }

    /**
     * Get all logs
     * @param ?string $queue Queue name. Default is null
     * @return array
     */
    public function all(?string $queue = null): array
    {
        return $this->withLock(function (array $records) use ($queue) {
            $filtered = $queue ? array_values(array_filter($records, fn($r) => $r['queue'] === $queue)) : $records;

            // Read only — no write
            return ['return' => $filtered];
        });
    }

    /**
     * Find Queue by ID
     * @param string $id Queue ID
     * @return ?array
     */
    public function find(string $id): ?array
    {
        return $this->withLock(function (array $records) use ($id) {
            foreach ($records as $r) {
                if ($r['id'] === $id) {
                    return ['return' => $r];
                }
            }

            // Read only — no write
            return ['return' => null];
        });
    }

    /**
     * Forget Queue by ID
     * @param string $id Queue ID
     * @return bool
     */
    public function forget(string $id): bool
    {
        return $this->withLock(function (array $records) use ($id) {
            $before = count($records);
            $records = array_values(array_filter($records, fn($r) => $r['id'] !== $id));
            return ['records' => $records, 'return' => count($records) < $before];
        });
    }

    /**
     * Flush queue
     * @param ?int $hours Hours. Example: 1
     * @return void
     */
    public function flush(?int $hours = null): void
    {
        $this->withLock(function (array $records) use ($hours) {
            if ($hours === null) {
                return ['records' => []];
            }
            $cutoff = time() - ($hours * 3600);
            $records = array_values(array_filter($records, fn($r) => $r['failed_at'] > $cutoff));
            return ['records' => $records];
        });
    }

    ####################################################################################
    /*================================= INTERNAL API =================================*/
    ####################################################################################
    /**
     * Run $fn against failed.json under one exclusive lock.
     * The callback returns ['records' => …] to write, 'return' => … to hand back.
     * Omitting 'records' skips the write entirely.
     */
    protected function withLock(callable $fn): mixed
    {
        return $this->store->mutate('failed', $fn);
    }
}
