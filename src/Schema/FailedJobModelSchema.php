<?php

namespace Laika\Queue\Schema;

use Laika\Model\Schema\Schema;
use Laika\Model\Schema\Blueprint;
use Laika\Model\Contract\SchemaAbstract;

/**
 * Not discovered by `php laika app:migrate`: call up() once to create the
 * table. It uses createIfNotExists, so repeating it is safe. Requires
 * laikait/laika-model, and is only autoloaded when something calls it, so
 * laika-queue does not need laika-model to run the json or redis driver.
 */
class FailedJobModelSchema extends SchemaAbstract
{
    /** @var string Database Table Name */
    protected string $table = 'laika_failed_jobs';

    /** @var string Database Connection Name */
    protected string $connection = 'default';

    public function __construct(?string $connection = null)
    {
        // No connection given: follow lf-config/queue.php, so the table
        // lands on the connection DatabaseFailedJobProvider reads it from
        $connection ??= function_exists('config') ? (string) config('queue', 'connection', 'default') : 'default';
        parent::__construct($connection);
    }

    public function up(): void
    {
        Schema::on($this->connection)->createIfNotExists($this->table, function (Blueprint $t) {
            $t->char('id', 36);
            $t->primary('id');
            $t->string('queue', 100);
            $t->longText('payload');
            $t->text('exception');
            $t->unsignedInteger('failed_at');
            $t->index('queue');
        });
    }
}
