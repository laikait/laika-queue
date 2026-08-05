# Laika Queue

Queue package for the [Laika PHP MVC Framework](https://github.com/laikait). Database, Redis, and JSON drivers with a signal-aware worker, delayed jobs, retry backoff, and failed-job tracking.

## Install

```bash
composer require laikait/laika-queue
```

## Usage

```php
use Laika\Queue\Driver\DatabaseDriver;
use Laika\Queue\Worker;

$driver = new DatabaseDriver($pdo);
$driver->push(new SendWelcomeEmail($userId), queue: 'emails', delay: 10);

$worker = new Worker($driver, $failedJobProvider);
$worker->work('emails');
```

Or via the Relay proxy once registered in your `ServiceProvider`:

```php
use Laika\Queue\QueueRelay as Queue;

Queue::push(new SendWelcomeEmail($userId));
```

## Drivers

| Driver | Storage | Notes |
|---|---|---|
| `DatabaseDriver` | PDO | `FOR UPDATE SKIP LOCKED` — MySQL 8+/PostgreSQL 9.5+ |
| `RedisDriver` | phpredis | Atomic pop via Lua script |
| `JsonDriver` | flat file | `flock`-based, good for local/dev |

## Jobs

```php
use Laika\Queue\Abstracts\Job;

class SendWelcomeEmail extends Job
{
    public int $maxTries = 3;
    protected int|array|null $backoffStrategy = [10, 30, 60];

    public function __construct(protected int $userId) {}

    public function handle(): void
    {
        // send email
    }

    public function failed(\Throwable $e): void
    {
        // log, notify, etc.
    }
}
```

Backoff: pass an `int` (linear), an `array` (per-attempt steps), or leave `null` for exponential (capped at 3600s). Set `$jitter = false;` to disable randomized spread.

## Worker

```bash
php bin/laika-worker default
```

Handles `SIGTERM`/`SIGINT` (graceful stop), `SIGUSR2`/`SIGCONT` (pause/resume), per-job timeout via `pcntl_fork`, and memory-limit auto-restart. Requires `pcntl`/`posix` (Linux/macOS) — degrades to no-timeout inline execution without them.

Keep the worker alive with supervisor or systemd (it self-exits on memory limit, and it's a long-running process, not a cron job):

```ini
[program:laika-queue-worker]
command=php /path/to/bin/laika-worker default
autostart=true
autorestart=true
numprocs=2
stopsignal=TERM
```

## Failed Jobs

Pass a `FailedJobProviderInterface` (`DatabaseFailedJobProvider` or `JsonFailedJobProvider`) to `Worker` — jobs exceeding `maxTries` are logged there instead of silently dropped.

## Known Gaps

- `DatabaseDriver`/`JsonDriver` have no stalled-job reaper (a worker dying mid-job leaves the row `reserved_at` forever). `RedisDriver::pop()` already self-heals via an internal sweep. A cron-callable `reapStalled()` for the other two drivers is a good next addition.
- `QueueRelay` and `QueueServiceProvider` assume a `getRelayAccessor()` / `register()`+`boot()` convention — adjust to match your actual `Laika\Core\Relay\Relay` and `ServiceProvider` base classes.

## Requirements

- PHP 8.1+
- `ext-redis` for `RedisDriver`
- `ext-pcntl` + `ext-posix` for full `Worker` signal/timeout support (Linux/macOS only)

## License

MIT
