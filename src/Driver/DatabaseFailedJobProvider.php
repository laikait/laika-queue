<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\FailedJobProviderInterface;
use PDO;

class DatabaseFailedJobProvider implements FailedJobProviderInterface
{
    protected PDO $pdo;
    protected string $table;

    public function __construct(PDO $pdo, string $table = 'laika_failed_jobs')
    {
        $this->pdo = $pdo;
        $this->table = $table;
        $this->ensureTable();
    }

    protected function ensureTable(): void
    {
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS {$this->table} (
            id CHAR(36) PRIMARY KEY,
            queue VARCHAR(100) NOT NULL,
            payload LONGTEXT NOT NULL,
            exception TEXT NOT NULL,
            failed_at INT UNSIGNED NOT NULL,
            INDEX idx_queue (queue)
        )");
    }

    public function log(string $queue, string $payload, \Throwable $e): string
    {
        $id = bin2hex(random_bytes(16));

        $stmt = $this->pdo->prepare("INSERT INTO {$this->table}
            (id, queue, payload, exception, failed_at)
            VALUES (:id, :queue, :payload, :exception, :failed_at)");

        $stmt->execute([
            'id' => $id,
            'queue' => $queue,
            'payload' => $payload,
            'exception' => (string) $e,
            'failed_at' => time(),
        ]);

        return $id;
    }

    public function all(?string $queue = null): array
    {
        if ($queue) {
            $stmt = $this->pdo->prepare("SELECT * FROM {$this->table} WHERE queue = :queue ORDER BY failed_at DESC");
            $stmt->execute(['queue' => $queue]);
        } else {
            $stmt = $this->pdo->query("SELECT * FROM {$this->table} ORDER BY failed_at DESC");
        }

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    public function find(string $id): ?array
    {
        $stmt = $this->pdo->prepare("SELECT * FROM {$this->table} WHERE id = :id");
        $stmt->execute(['id' => $id]);
        return $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
    }

    public function forget(string $id): bool
    {
        $stmt = $this->pdo->prepare("DELETE FROM {$this->table} WHERE id = :id");
        $stmt->execute(['id' => $id]);
        return $stmt->rowCount() > 0;
    }

    public function flush(?int $hours = null): void
    {
        if ($hours === null) {
            $this->pdo->exec("TRUNCATE TABLE {$this->table}");
            return;
        }

        $stmt = $this->pdo->prepare("DELETE FROM {$this->table} WHERE failed_at <= :cutoff");
        $stmt->execute(['cutoff' => time() - ($hours * 3600)]);
    }
}
