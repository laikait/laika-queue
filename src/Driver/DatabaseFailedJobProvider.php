<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\FailedJobProviderInterface;
use Laika\Queue\Model\FailedJobModel;

/**
 * Failed-job log backed by laika-model (see laikait/laika-model) via
 * FailedJobModel. Register the connection with Laika\Model\Connection::add(...)
 * before constructing this.
 *
 * The table isn't created here — run `php laika app:migrate` (which
 * discovers Laika\Queue\Schema\FailedJobModelSchema via helpers/loader.php),
 * or call Schema::on($connection)->createIfNotExists(...) yourself before use.
 */
class DatabaseFailedJobProvider implements FailedJobProviderInterface
{
    protected FailedJobModel $model;
    protected string $connection;

    public function __construct(string $connection = 'default')
    {
        $this->connection = $connection;
        $this->model = (new FailedJobModel($connection));
    }

    /**
     * Create the failed-jobs table if it doesn't exist yet, via
     * Laika\Queue\Schema\FailedJobModelSchema (Schema::createIfNotExists()
     * under the hood, so this is safe to call every time — not just once).
     *
     * An alternative to `php laika app:migrate` for callers that aren't
     * running inside a full Laika app (or just want the provider to be
     * self-sufficient). Requires laikait/laika-core — see README — since
     * that's what FailedJobModelSchema's base class lives in; this package
     * itself doesn't hard-depend on it.
     */
    public function ensureSchema(): void
    {
        (new \Laika\Queue\Schema\FailedJobModelSchema($this->connection))->up();
    }

    public function log(string $queue, string $payload, \Throwable $e): string
    {
        $id = bin2hex(random_bytes(16));

        $this->model->insert([
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
        $query = $this->model->order('failed_at', 'DESC');

        if ($queue) {
            $query = $query->where(['queue' => $queue]);
        }

        return $query->get();
    }

    public function find(string $id): ?array
    {
        $row = $this->model->where(['id' => $id])->first();
        return $row ?: null;
    }

    public function forget(string $id): bool
    {
        return $this->model->where(['id' => $id])->delete() > 0;
    }

    public function flush(?int $hours = null): void
    {
        if ($hours === null) {
            $this->model->notNull('id')->delete();
            return;
        }

        $this->model
            ->where(['failed_at' => time() - ($hours * 3600)], '<=')
            ->delete();
    }
}
