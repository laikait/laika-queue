# Laika Queue

Queue package for the [Laika PHP MVC Framework](https://github.com/laikait). Database, Redis, and JSON drivers with a signal-aware worker, delayed jobs, retry backoff, and failed-job tracking.

## Install

```bash
composer require laikait/laika-queue
```

The first time Composer builds the autoloader (`composer install`/`update`)
inside a Laika app, a `worker` executable is generated automatically in your
project root — no manual step needed:

```bash
php worker default
```

> Composer 2.2+ requires explicit trust for packages that ship a plugin. If
> you see a warning about `laikait/laika-queue` not being allowed to run
> code, add it to your project's `composer.json`:
> ```json
> "config": {
>     "allow-plugins": {
>         "laikait/laika-queue": true
>     }
> }
> ```
> (Already set up for you if you started from `laikait/laika-framework`.)

### Global install
Prefer a single `worker` command available in every project? Install it
globally instead:
```bash
composer global require laikait/laika-queue
```
Make sure Composer's global `vendor/bin` directory is on your `PATH` (see the
[Composer docs](https://getcomposer.org/doc/03-cli.md#global)), then run
`worker` from inside any Laika project directory (or a sub-directory of it):
```bash
worker default
```
The global binary detects the current project by walking up from your
working directory until it finds `lf-boot/app.php` — no `php` prefix needed.

## Usage

`DatabaseDriver` and `DatabaseFailedJobProvider` are built on [laikait/laika-model](https://github.com/laikait/laika-model) — register a connection by name with `Laika\Model\Connection` first, then hand the driver that name (it doesn't open PDO itself, and never accepts a raw connection object):

```php
use Laika\Model\Connection;
use Laika\Queue\Driver\DatabaseDriver;
use Laika\Queue\Worker;

Connection::add([
    'driver'   => 'mysql',
    'host'     => '127.0.0.1',
    'database' => 'myapp',
    'username' => 'root',
    'password' => 'secret',
]);

$driver = new DatabaseDriver('default'); // connection name, not a PDO instance
$driver->push(new SendWelcomeEmail($userId), queue: 'emails', delay: 10);

$worker = new Worker($driver, $failedJobProvider);
$worker->work('emails');
```

**`DatabaseDriver`/`DatabaseFailedJobProvider` don't create their table by themselves — call `ensureSchema()` once you've constructed them.** The `worker` executable already does this for you whenever `driver`/`failed_driver` resolves to `'database'` (see [Worker](#worker) below); construct the classes directly and you need to call it yourself:

```php
// Idempotent (CREATE TABLE IF NOT EXISTS under the hood), safe to call
// every time — targets whichever connection you constructed it with.
$driver->ensureSchema();
$failer->ensureSchema();
```

That's `Laika\Queue\Schema\QueueModelSchema`/`FailedJobModelSchema` under the hood — reach for those directly instead if you'd rather:

```php
use Laika\Queue\Schema\QueueModelSchema;
use Laika\Queue\Schema\FailedJobModelSchema;

(new QueueModelSchema('default'))->up();
(new FailedJobModelSchema('default'))->up();
```

Either way requires `laikait/laika-core` (it's what `QueueModelSchema`/`FailedJobModelSchema` extend) — or run `php laika app:migrate` (see below), which discovers both automatically with no code needed.

### Models & Schemas (`php laika app:migrate`)

`DatabaseDriver` queries through `Laika\Queue\Model\QueueModel`, and `DatabaseFailedJobProvider` through `Laika\Queue\Model\FailedJobModel` — both plain `Laika\Model\Model` subclasses, same convention as any other app model (`$table`/`$id`/`$connection`).

Alongside them, `Laika\Queue\Schema\QueueModelSchema` and `Laika\Queue\Schema\FailedJobModelSchema` extend `Laika\Core\Abstracts\SchemaAbstract` and own the actual table DDL. `helpers/loader.php` registers both directories with the framework's resource loader on install, so if this package is installed inside a Laika app (with `laikait/laika-core` present), `php laika app:migrate` discovers them automatically alongside your own `lf-app/Schema` classes — no wiring needed. That's now the only place these two tables get created; the drivers themselves stay framework-agnostic and just query.

`laikait/laika-core` is *not* a hard dependency of laika-queue: the Schema classes are plain PSR-4 files, lazily autoloaded only when something actually references them — constructing `DatabaseDriver`/`DatabaseFailedJobProvider` on their own never touches them. Calling `ensureSchema()` does (see [above](#usage)) — and the `worker` executable calls it for you, so running `worker` with a `database` driver does pull `laikait/laika-core` in, just not the bare classes by themselves.

### `pop()` has no locking clause — read this before running multiple workers

`pop()` claims a row with a plain `SELECT` (filtered to unreserved, due jobs) followed by an `UPDATE`, both inside a transaction, entirely through laika-model's portable query builder — no raw SQL, no per-driver branching, works identically on every driver laika-model supports (mysql, mariadb, pgsql, sqlite, sqlsrv, oci, firebird).

The tradeoff: there's no `FOR UPDATE`/`SKIP LOCKED` (or equivalent) row lock. Two `Worker` processes calling `pop()` on the same queue at close to the same moment can both read the same unreserved row before either commits its `UPDATE`, and both end up processing that job. If you only ever run a single worker process per queue, this never matters. If you run multiple (`numprocs=2` below, or several hosts), it can — decide based on whether your jobs are safe to run twice (idempotent), and add a driver-appropriate locking clause back into `pop()` yourself if not.

A `Laika\Queue\QueueRelay` / `QueueServiceProvider` pair for wiring this into the [Laika framework](https://github.com/laikait)'s container/facade convention isn't included yet — their base classes (`Laika\Core\Relay\Relay`, `Laika\Core\Container\ServiceProvider`) live in that framework, not here. Add them yourself once you're integrating against a real Laika core, following its actual Relay/ServiceProvider conventions.

## Drivers

| Driver | Storage | Notes |
|---|---|---|
| `DatabaseDriver` | [laika-model](https://github.com/laikait/laika-model) (PDO) | Portable across every laika-model driver; no locking clause on `pop()` — see [below](#pop-has-no-locking-clause--read-this-before-running-multiple-workers) |
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

### Security: trusted job classes

Job payloads are restored with PHP's `unserialize()`. To avoid PHP Object Injection (an attacker-controlled payload instantiating arbitrary classes), `Job::unserializePayload()` only allows classes you've explicitly registered — by default it allows none, and throws rather than silently unserializing something unexpected:

```php
use Laika\Queue\Abstracts\Job;

Job::registerTrustedClasses([
    SendWelcomeEmail::class,
    ChargeCard::class,
]);
```

Do this once at bootstrap, before any driver's `pop()` is called. The `worker` executable reads this list from `lf-config/queue.php`'s `trusted_job_classes` key via the framework's `config()` helper.

## Worker

```bash
php worker default
```

(`vendor/laikait/laika-queue/bin/worker default` also works directly — the project-root `worker` executable above is just a thin proxy to it, generated automatically, same idea as `laikait/laika-cli`'s `laika` entrypoint.)

The queue name (`default` above) is `$argv[1]`, and it's optional — plain `php worker` with no argument falls back to `'default'` too. One worker process handles exactly one queue name; run it multiple times (with different `numprocs`/program blocks below) for more than one. There's no validation against `lf-config/queue.php` or anything else — a typo'd name (`php worker emials`) doesn't error, it just runs forever quietly popping nothing.

### Choosing a driver

The `worker` executable doesn't hardcode a backend — it reads `lf-config/queue.php`'s `driver` key and builds whichever of the three ships with this package:

```php
// lf-config/queue.php
return [
    'driver' => 'json', // 'json' (default) | 'database' | 'redis'
    // ...
];
```

- **`database`** — `DatabaseDriver` against `connection` (default: `'default'`, see `lf-config/database.php`).
- **`redis`** — `RedisDriver::fromConfig()`, connecting with the app's existing `lf-config/redis.php` as-is (no separate queue connection to configure). Keys are namespaced under that file's `prefix` + `:queue`, so queue keys can't collide with cache/session keys sharing the same prefix. Requires `ext-redis`.
- **`json`** — `JsonDriver`, no config: always `lf-storage/queues/jobs.json` (every queue in one file, filtered by a `queue` field per record).

Failed jobs are wired independently via `failed_driver` (`'database'` | `'json'`) since there's no Redis-backed failed-job provider — it defaults to `'database'` when `driver` is `'database'`, and to `'json'` otherwise. See `lf-config/queue.php` for the full set of keys.

Whichever side (`driver` and/or `failed_driver`) resolves to `'database'`, the worker calls that class's `ensureSchema()` right after constructing it — `laika_queue_jobs`/`laika_failed_jobs` get created automatically on first run, no `php laika app:migrate` needed first.

Handles `SIGTERM`/`SIGINT` (graceful stop), `SIGUSR2`/`SIGCONT` (pause/resume), per-job timeout via `pcntl_fork`, and memory-limit auto-restart. Requires `pcntl`/`posix` (Linux/macOS) — degrades to no-timeout inline execution without them. A job that times out repeatedly is subject to the same `maxTries` limit as a job that throws — once exhausted it's logged to the failed-job provider instead of being released forever.

Memory limits come from the framework's own `CLI_MEMORY_LIMIT` (`lf-inc/const.php`), not a hardcoded value: `worker` applies it as this process's actual `memory_limit` ini ceiling via `Laika\Core\System\MemoryManager::apply()` (a no-op if `laikait/laika-core` isn't installed). `Worker::work()` doesn't need to be told that value — leave its `memoryLimit` argument as `null` (the default) and it reads PHP's *current* `memory_limit` itself (again via `MemoryManager`, when installed) and auto-derives a soft-restart threshold at ~90% of it, so the graceful auto-restart above triggers before that ceiling risks a hard OOM mid-job. Pass an explicit `int` instead if you want to override it. Falls back to a flat 128MB when `laikait/laika-core` isn't installed, or `memory_limit` is unlimited (`-1`).

The worker only runs inside a Laika app (it requires `lf-boot/app.php`): it sources trusted job classes from `lf-config/queue.php` and the DB connection from `lf-config/database.php` automatically — `Laika\Model\Model` self-connects via `Laika\Core\Helper\Init::db()` the moment `DatabaseDriver` constructs its model, so there's nothing else to configure here.

`RedisDriver` implements `ReconnectableDriver`: in the freshly forked child, `Worker` calls its `reconnect()`, which drops and reopens the connection so the child doesn't share the parent's socket — build it with `::fromConfig()` (not an already-open `Redis` instance) if it'll run inside a forking `Worker`. `DatabaseDriver`/`DatabaseFailedJobProvider` don't implement this — laika-model's `Connection` registry owns the PDO connection's lifecycle, not the driver.

Keep the worker alive with supervisor or systemd (it self-exits on memory limit, and it's a long-running process, not a cron job):

```ini
[program:laika-queue-worker]
command=php /path/to/project/worker default
autostart=true
autorestart=true
numprocs=2
stopsignal=TERM
```

## Failed Jobs

Pass a `FailedJobProviderInterface` (`DatabaseFailedJobProvider` or `JsonFailedJobProvider`) to `Worker` — jobs exceeding `maxTries` are logged there instead of silently dropped.

## Known Gaps

- `DatabaseDriver`/`JsonDriver` have no stalled-job reaper (a worker dying mid-job leaves the row `reserved_at` forever). `RedisDriver::pop()` already self-heals via an internal sweep. A cron-callable `reapStalled()` for the other two drivers is a good next addition.
- `DatabaseDriver::pop()` has no locking clause, so it isn't safe against multiple concurrent workers on the same queue without idempotent jobs — see [above](#pop-has-no-locking-clause--read-this-before-running-multiple-workers).
- No `QueueRelay`/`QueueServiceProvider` yet — see [Usage](#usage) above.

## Requirements

- PHP 8.1+
- [`laikait/laika-model`](https://github.com/laikait/laika-model) — required for `DatabaseDriver`/`DatabaseFailedJobProvider`, same as `laikait/laika-core` for `ensureSchema()`/`app:migrate`: not declared in `composer.json` at all, since `RedisDriver`/`JsonDriver` don't need it — install it yourself if you're using the database backend.
- `ext-redis` for `RedisDriver`
- `ext-pcntl` + `ext-posix` for full `Worker` signal/timeout support (Linux/macOS only)

## License

MIT
