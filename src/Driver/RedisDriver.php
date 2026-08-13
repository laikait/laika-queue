<?php

declare(strict_types=1);

namespace Laika\Queue\Driver;

use Laika\Core\Storage\Connection\RedisConnection;
use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Interfaces\ReconnectableDriver;
use Laika\Queue\Abstracts\Job;
use Redis;

/**
 * Redis queue driver.
 *
 * Clock note: scores are written from PHP's clock (push/release compute
 * time()+delay) and compared against Redis's clock inside the Lua scripts
 * (redis.call('TIME')). Comparing server-side keeps every worker's sweep
 * consistent with each other; the remaining boundary is app-vs-Redis drift on
 * the deadline itself, which only shifts a delay by that drift.
 */
class RedisDriver implements QueueDriverInterface, ReconnectableDriver
{
    protected Redis $redis;
    protected string $prefix;

    /** How long a reserved job may sit before it's treated as stalled. */
    protected int $reserveTimeout;

    /** Set when constructed via fromConfig(); lets reconnect() open a fresh connection post-fork. */
    protected ?\Closure $connector = null;

    /**
     * Claim the next available job.
     * KEYS: pending, reserved, jobs   ARGV: (none — reservation is scored with Redis TIME)
     */
    protected const POP_SCRIPT = <<<'LUA'
        local id = redis.call('LPOP', KEYS[1])
        if not id then return false end
        local now = tonumber(redis.call('TIME')[1])
        redis.call('ZADD', KEYS[2], now, id)
        local payload = redis.call('HGET', KEYS[3], id)
        if not payload then
            redis.call('ZREM', KEYS[2], id)
            return false
        end
        return {id, payload}
        LUA;

    /**
     * Move delayed jobs whose time has come back onto pending.
     * KEYS: delayed, pending   ARGV: (none)
     */
    protected const MIGRATE_SCRIPT = <<<'LUA'
        local now = tonumber(redis.call('TIME')[1])
        local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
        for i, id in ipairs(ids) do
            redis.call('ZREM', KEYS[1], id)
            redis.call('RPUSH', KEYS[2], id)
        end
        return #ids
        LUA;

    /**
     * Requeue reserved jobs that have stalled, but stop re-delivering poison jobs.
     * The counter is NOT incremented here — pop() already counts one per delivery,
     * so this only reads it and diverts to the dead set once the job has had its
     * allowance. maxTries of 0 disables the cap.
     * KEYS: reserved, pending, attempts, dead   ARGV: timeout, maxTries
     */
    protected const REAP_SCRIPT = <<<'LUA'
        local now = tonumber(redis.call('TIME')[1])
        local cutoff = now - tonumber(ARGV[1])
        local maxTries = tonumber(ARGV[2])
        local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff)
        local reaped = 0
        for i, id in ipairs(ids) do
            redis.call('ZREM', KEYS[1], id)
            local tries = tonumber(redis.call('HGET', KEYS[3], id)) or 0
            if maxTries > 0 and tries >= maxTries then
                redis.call('ZADD', KEYS[4], now, id)
            else
                redis.call('RPUSH', KEYS[2], id)
                reaped = reaped + 1
            end
        end
        return reaped
        LUA;

    /**
     * Store the payload and enqueue the id in one shot, so a crash can't
     * orphan a payload in the jobs hash with nothing pointing at it.
     * Clears any stale attempt count, so re-pushing an id (queue:retry on a
     * job that reached the dead set) starts from a clean slate.
     * KEYS: jobs, target (pending list or delayed zset), attempts
     * ARGV: id, payload, score (empty = immediate)
     */
    protected const PUSH_SCRIPT = <<<'LUA'
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        redis.call('HDEL', KEYS[3], ARGV[1])
        if ARGV[3] == '' then
            redis.call('RPUSH', KEYS[2], ARGV[1])
        else
            redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
        end
        return 1
        LUA;

    public function __construct(Redis $redis, string $prefix = 'laika_queue', ?int $reserveTimeout = null)
    {
        $this->redis = $redis;
        $this->prefix = $prefix;
        $this->reserveTimeout = $reserveTimeout ?? (int) config('queue', 'reserve_timeout', 90);
    }

    /**
     * Build a driver that can reconnect() with a brand-new connection — use
     * this instead of the plain constructor when the driver will be used
     * inside a Worker that forks (reconnect() is called in the forked child
     * so it doesn't share the parent's socket).
     *
     * Connection settings come from lf-config/redis.php via RedisConnection,
     * so timeouts, ACL usernames and error handling match RedisStorage.
     * $config entries override individual keys; 'auth' is a deprecated alias
     * for 'password'.
     */
    public static function fromConfig(array $config = [], string $prefix = 'laika_queue'): self
    {
        $connector = static fn (): Redis => RedisConnection::make($config);

        $driver = new self($connector(), $prefix);
        $driver->connector = $connector;

        return $driver;
    }

    public function reconnect(): void
    {
        if ($this->connector) {
            $this->redis = ($this->connector)();
        }
    }

    protected function pendingKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:pending";
    }

    protected function delayedKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:delayed";
    }

    protected function reservedKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:reserved";
    }

    protected function jobsKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:jobs";
    }

    protected function attemptsKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:attempts";
    }

    protected function deadKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:dead";
    }

    public function push(Job $job, string $queue = 'default', int $delay = 0): string
    {
        $job->id = $job->id ?: bin2hex(random_bytes(16));
        $job->queue = $queue;

        $target = $delay > 0 ? $this->delayedKey($queue) : $this->pendingKey($queue);
        $score  = $delay > 0 ? (string) (time() + $delay) : '';

        $this->redis->eval(self::PUSH_SCRIPT, [
            $this->jobsKey($queue), $target, $this->attemptsKey($queue),
            $job->id, $job->serializePayload(), $score,
        ], 3);

        return $job->id;
    }

    protected function migrateDelayed(string $queue): void
    {
        $this->redis->eval(self::MIGRATE_SCRIPT, [
            $this->delayedKey($queue), $this->pendingKey($queue),
        ], 2);
    }

    protected function reapStalled(string $queue, ?int $timeout = null): void
    {
        $this->redis->eval(self::REAP_SCRIPT, [
            $this->reservedKey($queue), $this->pendingKey($queue),
            $this->attemptsKey($queue), $this->deadKey($queue),
            $timeout ?? $this->reserveTimeout, $this->maxTries(),
        ], 4);
    }

    /**
     * Attempt ceiling used when reaping stalled jobs. 0 disables the cap.
     */
    protected function maxTries(): int
    {
        return (int) config('queue', 'max_tries', 3);
    }

    public function pop(string $queue = 'default'): ?Job
    {
        $this->migrateDelayed($queue);
        $this->reapStalled($queue);

        $result = $this->redis->eval(self::POP_SCRIPT, [
            $this->pendingKey($queue), $this->reservedKey($queue), $this->jobsKey($queue),
        ], 3);

        if (!$result) {
            return null;
        }

        [$id, $payload] = $result;

        // Attempts live in Redis so a reclaim by another worker still counts
        $tries = (int) $this->redis->hIncrBy($this->attemptsKey($queue), $id, 1);

        $job = Job::unserializePayload($payload);
        $job->id = $id;
        $job->queue = $queue;
        $job->tries = $tries;

        return $job;
    }

    public function ack(string $id, string $queue = 'default'): void
    {
        $this->redis->multi()
            ->zRem($this->reservedKey($queue), $id)
            ->hDel($this->jobsKey($queue), $id)
            ->hDel($this->attemptsKey($queue), $id)
            ->exec();
    }

    public function release(string $id, string $queue = 'default', int $delay = 0): void
    {
        $pipe = $this->redis->multi()->zRem($this->reservedKey($queue), $id);
        if ($delay > 0) {
            $pipe->zAdd($this->delayedKey($queue), time() + $delay, $id);
        } else {
            $pipe->rPush($this->pendingKey($queue), $id);
        }
        $pipe->exec();
    }

    public function delete(string $id, string $queue = 'default'): void
    {
        $this->ack($id, $queue);
    }

    public function size(string $queue = 'default'): int
    {
        // Matches DatabaseDriver/JsonDriver semantics: every unreserved job,
        // including ones delayed into the future — not just immediately
        // available ones.
        return $this->redis->lLen($this->pendingKey($queue))
            + $this->redis->zCard($this->delayedKey($queue));
    }
}
