<?php

namespace Laika\Queue\Model;

use Laika\Model\Model;

/**
 * Model for the failed-jobs table. FailedJobModelSchema creates it.
 */
class FailedJobModel extends Model
{
    /** @var string Table Name */
    protected string $table = 'laika_failed_jobs';

    /** @var string Primary Column Name */
    protected string $id = 'id';

    /** @var string Database Connection Name */
    protected string $connection = 'default';
}
