<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Abstracts\Job;
use Laika\Queue\Model\QueueModel;
use Laika\Queue\Schema\QueueModelSchema;
use Laika\Queue\Exceptions\DriverException;
use Laika\Queue\Interfaces\QueueDriverInterface;

/**
 * Queue driver backed by laika-model (see laikait/laika-model) via QueueModel.
 * Register the connection with Laika\Model\Connection::add(...) before
 * constructing this — the driver only refers to it by name, it never opens
 * PDO itself.
 *
 * The table isn't created here — run `php laika app:migrate` (which
 * discovers Laika\Queue\Schema\QueueModelSchema via helpers/loader.php), or
 * call Schema::on($connection)->createIfNotExists(...) yourself before use.
 *
 * pop() claims a row with a plain SELECT + UPDATE inside a transaction, no
 * locking clause (no FOR UPDATE / SKIP LOCKED) — this keeps it driver-agnostic
 * across everything laika-model supports, but it means two workers popping
 * concurrently can both read the same unreserved row before either commits
 * its UPDATE, and both end up processing the same job. If that matters for
 * your workload, either run a single worker process per queue, or add a
 * locking clause back in for your specific database driver.
 */
class DatabaseDriver implements QueueDriverInterface
{
    protected QueueModel $model;
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
        $this->model = new QueueModel($connection);
    }

    /**
     * Install Database Driver
     * @return void
     */
    public function install(): void
    {
        $schema = new QueueModelSchema($this->connection);
        $schema->down();
        $schema->up();
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

        $this->model->insert([
            'id' => $job->id,
            'queue' => $queue,
            'payload' => $job->serializePayload(),
            'attempts' => 0,
            'reserved_at' => null,
            'available_at' => $now + $delay,
            'created_at' => $now,
        ]);

        return $job->id;
    }

    /**
     * Pop queue jobs
     * @param string $queue Queue
     * @return ?Job
     */
    public function pop(string $queue = 'default'): ?Job
    {
        return $this->model->transaction(function (QueueModel $model) use ($queue) {
            $now = time();

            $rows = $model
                ->where(['queue' => $queue])
                ->isNull('reserved_at')
                ->where(['available_at' => $now], '<=')
                ->order('available_at', 'ASC')
                ->limit(1)
                ->get();

            if (!$rows) {
                return null;
            }

            $row = $rows[0];

            $model->where(['id' => $row['id']])->update(['reserved_at' => $now]);
            $model->where(['id' => $row['id']])->increment('attempts');

            $job = Job::unserializePayload($row['payload']);
            $job->id = $row['id'];
            $job->queue = $row['queue'];
            $job->tries = (int) $row['attempts'] + 1;

            return $job;
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
        $this->model
            ->where(['id' => $id])
            ->where(['queue' => $queue])
            ->update([
                'reserved_at' => null,
                'available_at' => time() + $delay,
            ]);
    }

    /**
     * Delete a single queue
     * @param string $id Queue job ID
     * @param string $queue Queue. Default is 'default'
     * @return void
     */
    public function delete(string $id, string $queue = 'default'): void
    {
        $this->model
            ->where(['id' => $id, 'queue' => $queue])
            ->delete();
    }

    /**
     * Size of Queue
     * @param string $queue Queue. Default is 'default'
     * @return int
     */
    public function size(string $queue = 'default'): int
    {
        return $this->model
            ->where(['queue' => $queue])
            ->isNull('reserved_at')
            ->count();
    }
}
