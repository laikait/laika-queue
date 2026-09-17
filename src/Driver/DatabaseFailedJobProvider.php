<?php

namespace Laika\Queue\Driver;

use Throwable;
use Laika\Queue\Model\FailedJobModel;
use Laika\Queue\Schema\FailedJobModelSchema;
use Laika\Queue\Interfaces\FailedJobProviderInterface;

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
    protected string $connection = 'default';

    public function __construct(?string $connection = null)
    {
        if (is_string($connection)) {
            $this->connection = match(true) {
                preg_match('/^[a-z]+$/i', $connection)  =>  strtolower($connection),
                default                                 =>  throw new DriverException(
                    "Invalid connection name [{$connection}]!"
                    )
            };
        }
        $this->model = new FailedJobModel($this->connection);
    }

    /**
     * Install Database Driver
     * @return void
     */
    public function install(): void
    {
        $schema = new FailedJobModelSchema($this->connection);
        $schema->down();
        $schema->up();
    }

    /**
     * Log Failed queue
     * @param string $queue Queue
     * @param string $payload Log payload
     * @param string $payload Log payload
     * @param Throwable $e Throwable object
     */
    public function log(string $queue, string $payload, Throwable $e): string
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

    /**
     * Get all logs
     * @param ?string $queue Queue name. Default is null
     * @return array
     */
    public function all(?string $queue = null): array
    {
        $query = $this->model->order('failed_at', 'DESC');

        if ($queue) {
            $query = $query->where(['queue' => $queue]);
        }

        return $query->get();
    }

    /**
     * Find Queue by ID
     * @param string $id Queue ID
     * @return ?array
     */
    public function find(string $id): ?array
    {
        $row = $this->model->where(['id' => $id])->first();
        return $row ?: null;
    }

    /**
     * Forget Queue by ID
     * @param string $id Queue ID
     * @return bool
     */
    public function forget(string $id): bool
    {
        return $this->model->where(['id' => $id])->delete() > 0;
    }

    /**
     * Flush queue
     * @param ?int $hours Hours. Example: 1
     * @return void
     */
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
