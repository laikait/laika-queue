<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Abstracts\Job;
use PDO;

class DatabaseDriver implements QueueDriverInterface
{
    protected PDO $pdo;
    protected string $table;

    public function __construct(PDO $pdo, string $table = 'laika_queue_jobs')
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
            attempts INT UNSIGNED NOT NULL DEFAULT 0,
            reserved_at INT UNSIGNED NULL,
            available_at INT UNSIGNED NOT NULL,
            created_at INT UNSIGNED NOT NULL,
            INDEX idx_queue_avail (queue, reserved_at, available_at)
        )");
    }

    public function push(Job $job, string $queue = 'default', int $delay = 0): string
    {
        $job->id = $job->id ?: bin2hex(random_bytes(16));
        $job->queue = $queue;

        $stmt = $this->pdo->prepare("INSERT INTO {$this->table}
            (id, queue, payload, attempts, reserved_at, available_at, created_at)
            VALUES (:id, :queue, :payload, 0, NULL, :available_at, :created_at)");

        $now = time();
        $stmt->execute([
            'id' => $job->id,
            'queue' => $queue,
            'payload' => $job->serializePayload(),
            'available_at' => $now + $delay,
            'created_at' => $now,
        ]);

        return $job->id;
    }

    public function pop(string $queue = 'default'): ?Job
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->prepare("SELECT * FROM {$this->table}
            WHERE queue = :queue AND reserved_at IS NULL AND available_at <= :now
            ORDER BY available_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED");
        $stmt->execute(['queue' => $queue, 'now' => time()]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if (!$row) {
            $this->pdo->commit();
            return null;
        }

        $upd = $this->pdo->prepare("UPDATE {$this->table}
            SET reserved_at = :now, attempts = attempts + 1 WHERE id = :id");
        $upd->execute(['now' => time(), 'id' => $row['id']]);

        $this->pdo->commit();

        $job = Job::unserializePayload($row['payload']);
        $job->id = $row['id'];
        $job->queue = $row['queue'];
        $job->tries = (int) $row['attempts'] + 1;

        return $job;
    }

    public function ack(string $id, string $queue = 'default'): void
    {
        $this->delete($id, $queue);
    }

    public function release(string $id, string $queue = 'default', int $delay = 0): void
    {
        $stmt = $this->pdo->prepare("UPDATE {$this->table}
            SET reserved_at = NULL, available_at = :available_at
            WHERE id = :id AND queue = :queue");
        $stmt->execute([
            'available_at' => time() + $delay,
            'id' => $id,
            'queue' => $queue,
        ]);
    }

    public function delete(string $id, string $queue = 'default'): void
    {
        $stmt = $this->pdo->prepare("DELETE FROM {$this->table} WHERE id = :id AND queue = :queue");
        $stmt->execute(['id' => $id, 'queue' => $queue]);
    }

    public function size(string $queue = 'default'): int
    {
        $stmt = $this->pdo->prepare("SELECT COUNT(*) FROM {$this->table} WHERE queue = :queue AND reserved_at IS NULL");
        $stmt->execute(['queue' => $queue]);
        return (int) $stmt->fetchColumn();
    }
}
