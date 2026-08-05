<?php

namespace Laika\Queue\Interfaces;

/**
 * Optional capability for drivers that hold a stateful connection (PDO,
 * phpredis). pcntl_fork() duplicates the parent's open connection fd into the
 * child without reconnecting, so both processes end up sharing one socket —
 * if the child is killed mid-write (e.g. on a Worker job timeout), the
 * socket can desync for the parent too. Worker calls reconnect() in the
 * freshly-forked child, before it runs the job, so the child gets its own
 * connection instead of sharing the parent's.
 */
interface ReconnectableDriver
{
    public function reconnect(): void;
}
