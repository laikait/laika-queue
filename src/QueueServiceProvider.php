<?php

namespace Laika\Queue;

use Laika\Core\Container\ServiceProvider;
use Laika\Queue\Driver\DatabaseDriver;
use PDO;

/**
 * NOTE: base namespace Laika\Core\Container\ServiceProvider and register()/boot()
 * method names are assumptions based on partial framework details — adjust to
 * match your actual ServiceProvider base class and container binding syntax.
 */
class QueueServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->bind('queue', function ($container) {
            return new DatabaseDriver($container->make(PDO::class));
        });
    }

    public function boot(): void
    {
        //
    }
}
