<?php

namespace Laika\Queue\Schema;

use Laika\Model\Schema\Schema;
use Laika\Model\Schema\Blueprint;
use Laika\Core\Abstracts\SchemaAbstract;

/**
 * Discovered by `php laika app:migrate` via the resource loader (see
 * helpers/loader.php). Requires laikait/laika-core — only autoloaded when
 * the framework's migrate command actually reaches for it, so laika-queue
 * itself does not need laika-core installed to function standalone.
 */
class QueueModelSchema extends SchemaAbstract
{
    /** @var string Database Table Name */
    protected string $table = 'laika_queue_jobs';

    /** @var string Database Connection Name */
    protected string $connection = 'default';

    public function up(): void
    {
        Schema::on($this->connection)->createIfNotExists($this->table, function(Blueprint $t) {
            $t->char('id', 36);
            $t->primary('id');
            $t->string('queue', 100);
            $t->longText('payload');
            $t->unsignedInteger('attempts')->default(0);
            $t->unsignedInteger('reserved_at')->nullable();
            $t->unsignedInteger('available_at');
            $t->unsignedInteger('created_at');
            $t->index(['queue', 'reserved_at', 'available_at']);
        });
    }
}
