<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\FailedJobProviderInterface;

class JsonFailedJobProvider implements FailedJobProviderInterface
{
    protected string $file;

    public function __construct()
    {
        $this->file = APP_PATH . '/lf-storage/queues/failed.json';

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

    public function all(?string $queue = null): array
    {
        return $this->withLock(function (array $records) use ($queue) {
            $filtered = $queue ? array_values(array_filter($records, fn($r) => $r['queue'] === $queue)) : $records;
            return ['records' => $records, 'return' => $filtered];
        });
    }

    public function find(string $id): ?array
    {
        return $this->withLock(function (array $records) use ($id) {
            foreach ($records as $r) {
                if ($r['id'] === $id) {
                    return ['records' => $records, 'return' => $r];
                }
            }
            return ['records' => $records, 'return' => null];
        });
    }

    public function forget(string $id): bool
    {
        return $this->withLock(function (array $records) use ($id) {
            $before = count($records);
            $records = array_values(array_filter($records, fn($r) => $r['id'] !== $id));
            return ['records' => $records, 'return' => count($records) < $before];
        });
    }

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
}
