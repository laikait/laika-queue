<?php

namespace Laika\Queue\Interfaces;

use Laika\Queue\Abstracts\Job;

interface QueueDriverInterface
{
    public function push(Job $job, string $queue = 'default', int $delay = 0): string;
    public function pop(string $queue = 'default'): ?Job;
    public function ack(string $id, string $queue = 'default'): void;
    public function release(string $id, string $queue = 'default', int $delay = 0): void;
    public function delete(string $id, string $queue = 'default'): void;
    public function size(string $queue = 'default'): int;
}
