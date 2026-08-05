<?php

namespace Laika\Queue\Driver;

use Laika\Queue\Interfaces\QueueDriverInterface;
use Laika\Queue\Interfaces\ReconnectableDriver;
use Laika\Queue\Abstracts\Job;
use Redis;

class RedisDriver implements QueueDriverInterface, ReconnectableDriver
{
    protected Redis $redis;
    protected string $prefix;

    /** Set when constructed via fromConfig(); lets reconnect() open a fresh connection post-fork. */
    protected ?\Closure $connector = null;

    protected const POP_SCRIPT = <<<'LUA'
        local id = redis.call('LPOP', KEYS[1])
        if not id then return false end
        redis.call('ZADD', KEYS[2], ARGV[1], id)
        local payload = redis.call('HGET', KEYS[3], id)
        if not payload then
            redis.call('ZREM', KEYS[2], id)
            return false
        end
        return {id, payload}
        LUA;

    protected const SWEEP_SCRIPT = <<<'LUA'
        local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
        for i, id in ipairs(ids) do
            redis.call('ZREM', KEYS[1], id)
            redis.call('RPUSH', KEYS[2], id)
        end
        return #ids
        LUA;

    public function __construct(Redis $redis, string $prefix = 'laika_queue')
    {
        $this->redis = $redis;
        $this->prefix = $prefix;
    }

    /**
     * Build a driver that can reconnect() with a brand-new connection — use
     * this instead of the plain constructor when the driver will be used
     * inside a Worker that forks (reconnect() is called in the forked child
     * so it doesn't share the parent's socket).
     */
    public static function fromConfig(array $config, string $prefix = 'laika_queue'): self
    {
        $connector = static function () use ($config): Redis {
            $redis = new Redis();
            $redis->connect(
                $config['host'] ?? '127.0.0.1',
                $config['port'] ?? 6379,
                $config['timeout'] ?? 0.0
            );
            if (!empty($config['auth'])) {
                $redis->auth($config['auth']);
            }
            if (isset($config['database'])) {
                $redis->select($config['database']);
            }
            return $redis;
        };

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

    public function push(Job $job, string $queue = 'default', int $delay = 0): string
    {
        $job->id = $job->id ?: bin2hex(random_bytes(16));
        $job->queue = $queue;

        $this->redis->hSet($this->jobsKey($queue), $job->id, $job->serializePayload());

        if ($delay > 0) {
            $this->redis->zAdd($this->delayedKey($queue), time() + $delay, $job->id);
        } else {
            $this->redis->rPush($this->pendingKey($queue), $job->id);
        }

        return $job->id;
    }

    protected function migrateDelayed(string $queue): void
    {
        $this->redis->eval(self::SWEEP_SCRIPT, [
            $this->delayedKey($queue), $this->pendingKey($queue), time(),
        ], 2);
    }

    protected function reapStalled(string $queue, int $timeout = 90): void
    {
        $this->redis->eval(self::SWEEP_SCRIPT, [
            $this->reservedKey($queue), $this->pendingKey($queue), time() - $timeout,
        ], 2);
    }

    public function pop(string $queue = 'default'): ?Job
    {
        $this->migrateDelayed($queue);
        $this->reapStalled($queue);

        $result = $this->redis->eval(self::POP_SCRIPT, [
            $this->pendingKey($queue), $this->reservedKey($queue), $this->jobsKey($queue), time(),
        ], 3);

        if (!$result) {
            return null;
        }

        [$id, $payload] = $result;

        $job = Job::unserializePayload($payload);
        $job->id = $id;
        $job->queue = $queue;
        $job->tries += 1;
        $this->redis->hSet($this->jobsKey($queue), $id, $job->serializePayload());

        return $job;
    }

    public function ack(string $id, string $queue = 'default'): void
    {
        $this->redis->multi()
            ->zRem($this->reservedKey($queue), $id)
            ->hDel($this->jobsKey($queue), $id)
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
