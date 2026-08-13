# Laika Queue

Background job processing for the [Laika PHP MVC Framework](https://github.com/laikait) — database, Redis, and JSON drivers behind one interface, with delayed jobs, retry backoff, failed-job tracking, and a signal-aware worker process.

This README is the full reference. If you just want the short version, the framework handbook has a [Queue chapter](https://github.com/laikait/laika-framework/blob/main/docs/12_queue/01_basic.md).

**Contents**

- [Install](#install)
- [Quick start](#quick-start) — the five-step loop
- [Configuration](#configuration)
- [Defining jobs](#defining-jobs)
- [Dispatching jobs](#dispatching-jobs)
- [CLI reference](#cli-reference)
- [Drivers](#drivers)
- [The worker](#the-worker)
- [Failed jobs](#failed-jobs)
- [Schema & migrations](#schema--migrations)
- [Production](#production)
- [Concurrency caveat](#concurrency-caveat)
- [Security: trusted job classes](#security-trusted-job-classes)
- [Known gaps](#known-gaps)
- [Standalone use](#standalone-use-outside-the-framework)
- [Requirements](#requirements)

---

## Install

```bash
composer require laikait/laika-queue
```

Inside a Laika app you normally get it already — `laikait/laika-core` depends on it.

Installing generates a `worker` executable in your project root (plus `worker.bat` on Windows), so the entry point always matches the version in `vendor/`:

| File | Platform | How you run it |
| --- | --- | --- |
| `worker` | all | `php worker default` — or `./worker default` on Linux/macOS |
| `worker.bat` | Windows only | `worker default` in cmd, `.\worker default` in PowerShell |

Both are thin proxies into `vendor/laikait/laika-queue/bin/worker`. They're rewritten only when their contents actually change, and regenerate if you delete them.

Generation is driven by `Laika\Queue\ScriptHandler::generate`. Composer only runs scripts declared by the **root** project, never by a dependency, so a project not created from the framework skeleton needs to wire it itself:

```json
"scripts": {
    "post-autoload-dump": [
        "Laika\\Queue\\ScriptHandler::generate"
    ],
    "post-create-project-cmd": [
        "Laika\\Queue\\ScriptHandler::generate"
    ]
}
```

The framework skeleton already has this. `ScriptHandler` is a safe no-op anywhere `lf-boot/app.php` isn't present, so a global install or the package's own CI won't trip over it.

> This package used to ship as a Composer *plugin*, which forced an `allow-plugins` entry into every consuming project. It's a plain library now — you can drop `"laikait/laika-queue": true` from your `config.allow-plugins`.

### Global install

Want one `worker` command across every project?

```bash
composer global require laikait/laika-queue
```

Put Composer's global `vendor/bin` on your `PATH` (see the [Composer docs](https://getcomposer.org/doc/03-cli.md#global)), then run `worker` from inside any Laika project:

```bash
worker default
```

The global binary finds the project by walking up from your working directory until it hits `lf-boot/app.php` — no `php` prefix needed, and it works from a sub-directory.

---

## Quick start

The whole loop, end to end.

**1. Create the job**

```bash
php laika job:make SendWelcomeEmail --queue=emails --max=3
```

That writes `lf-app/Job/SendWelcomeEmail.php`.

**2. Fill in `handle()`**

```php
namespace App\Job;

use Laika\Queue\Abstracts\Job;

class SendWelcomeEmail extends Job
{
    public string $queue = 'emails';
    public int $maxTries = 3;

    public function __construct(protected int $userId) {}

    public function handle(): void
    {
        // send the email
    }

    public function failed(\Throwable $e): void
    {
        // log, notify, etc.
    }
}
```

**3. Pick a backend** in `lf-config/queue.php` — `'json'` needs no setup and is the default:

```php
return [
    'driver' => 'json', // 'json' | 'database' | 'redis'
];
```

**4. Dispatch it** from a controller, service, anywhere:

```php
use Laika\Cli\QueueResolver;
use App\Job\SendWelcomeEmail;

QueueResolver::driver()->push(new SendWelcomeEmail($userId), 'emails');
```

**5. Run the worker**

```bash
php laika queue:work emails
```

That's it. The job runs, retries on failure with backoff, and lands in the failed-job log once it exhausts `maxTries` — inspect it with `php laika queue:failed`.

---

## Configuration

Everything lives in `lf-config/queue.php`:

```php
return [
    // Which backend the worker runs against: 'database' | 'redis' | 'json'.
    'driver' => 'json',

    // Connection name (see lf-config/database.php), used when 'driver' or
    // 'failed_driver' is 'database'.
    'connection' => 'default',

    // Failed-job provider: 'database' | 'json'. null mirrors 'driver'.
    'failed_driver' => null,
];
```

| Key | Default | Notes |
| --- | --- | --- |
| `driver` | `'database'` when the key is absent entirely; the shipped config file sets `'json'` | Selects `DatabaseDriver`, `RedisDriver`, or `JsonDriver` |
| `connection` | `'default'` | A connection *name* from `lf-config/database.php`, never a PDO instance |
| `failed_driver` | `null` | `null` resolves to `'database'` when `driver` is `'database'`, otherwise `'json'` |

**Why `failed_driver` is separate:** there is no Redis-backed failed-job provider. A `redis` queue driver still has to log its failures somewhere, so it falls back to `json` unless you say `'database'`.

The `redis` and `json` drivers have no config of their own:

- **`redis`** connects with `lf-config/redis.php` as-is (`host`, `port`, `password`). Keys are namespaced under that file's `prefix` + `:queue`, so queue keys can't collide with cache or session keys sharing the prefix.
- **`json`** always uses `lf-storage/queues/jobs.json` (and `lf-storage/queues/failed.json`). Every queue shares one file, filtered by a `queue` field per record.

This same resolution logic lives in two places, kept deliberately in sync: `bin/worker` (for the worker process) and `Laika\Cli\QueueResolver` (for the `queue:*` commands and your own dispatch code).

---

## Defining jobs

Extend `Laika\Queue\Abstracts\Job` and implement `handle()`. Use `php laika job:make` to scaffold one.

```php
use Laika\Queue\Abstracts\Job;

class ChargeCard extends Job
{
    public int $maxTries = 5;
    public int $retryAfter = 30;
    protected int|array|null $backoffStrategy = [10, 30, 60];
    protected bool $jitter = true;

    public function __construct(protected string $invoiceId) {}

    public function handle(): void
    {
        // do the work; throw to trigger a retry
    }

    public function failed(\Throwable $e): void
    {
        // called after EVERY failed attempt — see the note below
    }
}
```

### Properties

| Property | Type | Default | Meaning |
| --- | --- | --- | --- |
| `$id` | `string` | `''` | Assigned by the driver on `push()`. Don't set it yourself. |
| `$queue` | `string` | `'default'` | See the trap below — `push()` overwrites this. |
| `$tries` | `int` | `0` | Current attempt number, set by the driver on `pop()`. |
| `$maxTries` | `int` | `3` | Attempts before the job is handed to the failed-job provider. |
| `$retryAfter` | `int` | `60` | Base seconds for exponential backoff. |
| `$delay` | `int` | `0` | **Not read by anything** — see the trap below. |
| `$backoffStrategy` | `int\|array\|null` | `null` | Retry spacing, see below. Protected. |
| `$jitter` | `bool` | `true` | Randomize backoff by ±20%. Protected. |

### Backoff

`backoff()` returns the seconds to wait before the next attempt:

| `$backoffStrategy` | Behaviour |
| --- | --- |
| `null` (default) | Exponential: `retryAfter * 2^(tries-1)`, capped at 3600s |
| `int` | Linear: `n * tries` |
| `array` | Per-attempt steps, e.g. `[10, 30, 60]`. Clamps to the last entry once attempts exceed the array; an empty array falls back to exponential |

With `$jitter = true` (the default) the result is spread by ±20% so a batch of jobs failing together doesn't retry in lockstep. Set `$jitter = false` for deterministic timing.

### Two traps worth knowing

**`failed()` fires on every failed attempt, not just the last one.** `Worker::process()` calls `$job->failed($e)` before it decides whether to retry or give up. If you only want the final-failure behaviour, check the counter yourself:

```php
public function failed(\Throwable $e): void
{
    if ($this->tries >= $this->maxTries) {
        // genuinely dead
    }
}
```

**`$queue` and `$delay` on the job object are not what routes it.** Every driver's `push()` signature is `push(Job $job, string $queue = 'default', int $delay = 0)`, and it *assigns* `$job->queue = $queue` from that argument. So a job declaring `public string $queue = 'emails';` — which is exactly what `job:make --queue=emails` generates — still gets pushed to `default` unless you pass the queue explicitly:

```php
$driver->push(new SendWelcomeEmail($id));            // -> 'default', ignores the property
$driver->push(new SendWelcomeEmail($id), 'emails');  // -> 'emails'
```

`$job->delay` is never read by any driver; delay is the third `push()` argument only.

---

## Dispatching jobs

### Inside the framework (recommended)

`Laika\Cli\QueueResolver` builds whichever driver `lf-config/queue.php` names, so your dispatch code doesn't hardcode a backend and doesn't drift from the worker's choice:

```php
use Laika\Cli\QueueResolver;

$driver = QueueResolver::driver();

// push(Job $job, string $queue = 'default', int $delay = 0): string
$id = $driver->push(new SendWelcomeEmail($userId), 'emails');

// 10 seconds from now
$driver->push(new SendWelcomeEmail($userId), 'emails', 10);
```

`QueueResolver` ships in `laikait/laika-cli`, a hard dependency of `laikait/laika-core`, so it's always available in a Laika app — web requests included, not just CLI. It also calls `ensureSchema()` for you when the driver is `database`.

### Constructing a driver directly

The escape hatch when you need a specific backend regardless of config:

```php
use Laika\Model\Connection;
use Laika\Queue\Driver\DatabaseDriver;

Connection::add(config('database', 'default'));
$driver = new DatabaseDriver('default'); // a connection NAME, not a PDO instance
$driver->ensureSchema();
$driver->push(new SendWelcomeEmail($userId), 'emails');
```

`DatabaseDriver` never opens PDO itself — it refers to a connection by name from [laika-model](https://github.com/laikait/laika-model)'s registry. Register the connection first.

### The driver interface

All three drivers implement `Laika\Queue\Interfaces\QueueDriverInterface`:

```php
public function push(Job $job, string $queue = 'default', int $delay = 0): string;
public function pop(string $queue = 'default'): ?Job;
public function ack(string $id, string $queue = 'default'): void;
public function release(string $id, string $queue = 'default', int $delay = 0): void;
public function delete(string $id, string $queue = 'default'): void;
public function size(string $queue = 'default'): int;
```

`size()` counts every unreserved job including ones delayed into the future — consistent across all three drivers.

---

## CLI reference

Eight commands ship with `laikait/laika-cli` for this package.

### Jobs

| Command | Description |
| --- | --- |
| `php laika job:make <name> [--queue=default] [--max=3]` | Scaffold `lf-app/Job/<name>.php` |
| `php laika job:list` | List every discovered `Job` subclass |
| `php laika job:remove <name>` | Delete a job class |
| `php laika job:rename <--old=name> <--new=name>` | Rename a job class |

`job:make` validates that the name and queue are alphabetic (`/^[a-z_]+$/i`) and that `--max` is a positive integer.

### Queue

| Command | Description |
| --- | --- |
| `php laika queue:work [queue]` | Run the worker in the foreground (default queue: `default`) |
| `php laika queue:failed [--queue=name]` | Table of failed jobs — id, queue, failed-at, first line of the exception |
| `php laika queue:retry <id>\|--all [--queue=name]` | Re-push failed job(s) onto their original queue |
| `php laika queue:flush [--hours=N]` | Clear failed jobs; `--hours` keeps anything newer than N hours |

Notes:

- **`queue:work`** is a thin proxy — it shells out to the root `worker` executable (falling back to `vendor/laikait/laika-queue/bin/worker`) so it can never drift from the real worker's driver selection, signal handling, or memory logic. `php worker default` and `php laika queue:work default` do the same thing.
- **`queue:retry`** resets `$job->tries = 0` before re-pushing. Without that, a job whose payload already carries an exhausted attempt count would fail permanently on its very next run under `RedisDriver`, which increments the payload's own counter rather than reading a stored column. It also registers trusted classes first, same as the worker.
- **`queue:flush`** asks for confirmation before deleting.

---

## Drivers

| Driver | Storage | Concurrency | Use when |
| --- | --- | --- | --- |
| `JsonDriver` | `lf-storage/queues/jobs.json` | `flock`, but see caveat | Local/dev, zero setup |
| `DatabaseDriver` | [laika-model](https://github.com/laikait/laika-model) (PDO) | No row lock, see caveat | You already have a database and want durability |
| `RedisDriver` | phpredis | Atomic — safe for multiple workers | Throughput, multiple concurrent workers |

### `RedisDriver`

The only driver genuinely safe for concurrent workers. `pop()` is a Lua script, so claiming a job is atomic. It keeps four keys per queue — `:pending` (list), `:delayed` (sorted set), `:reserved` (sorted set), `:jobs` (hash of payloads) — and every `pop()` first migrates due delayed jobs into pending, then sweeps reserved jobs older than 90 seconds back into pending. That sweep is what makes it self-healing when a worker dies mid-job.

Build it with `RedisDriver::fromConfig()` rather than handing it an open `Redis` instance if it'll run inside a forking worker — see [`ReconnectableDriver`](#the-worker). Requires `ext-redis`.

### `DatabaseDriver`

Queries through `Laika\Queue\Model\QueueModel` using laika-model's portable query builder — no raw SQL, no per-driver branching, so it works identically on every backend laika-model supports (mysql, mariadb, pgsql, sqlite, sqlsrv, oci, firebird). The tradeoff is the missing row lock; see [Concurrency caveat](#concurrency-caveat).

### `JsonDriver`

One JSON array of records in a single file, guarded by `flock()` on every read-modify-write. Fine for development, not intended for production. Note it depends on the framework globals `APP_PATH` and `setPermission()`, so unlike the other two it **cannot** be used outside a Laika app.

---

## The worker

```bash
php laika queue:work emails    # or: php worker emails
```

The queue name is optional and defaults to `default`. **One worker process handles exactly one queue name** — run it multiple times for more than one queue. There's no validation against your config: a typo'd name doesn't error, it just runs forever popping nothing.

`Worker::work()` takes four arguments:

```php
$worker->work(
    queue: 'default',
    sleep: 3,        // seconds to idle when the queue is empty
    timeout: 60,     // per-job wall-clock limit
    memoryLimit: null // MB; null = auto-derive
);
```

### Signals

| Signal | Effect |
| --- | --- |
| `SIGTERM`, `SIGINT` | Graceful stop — finishes the current job, then exits |
| `SIGUSR2` | Pause (keeps running, stops popping) |
| `SIGCONT` | Resume |

Requires `ext-pcntl`. Without it the worker still runs, it just can't be signalled.

### Per-job timeout

With `pcntl` available, each job runs in a forked child. If the child exceeds `timeout`, the parent `SIGKILL`s it and treats it as a failed attempt — released with backoff, or logged to the failed-job provider once `maxTries` is exhausted. So a job that hangs repeatedly is subject to the same retry ceiling as one that throws.

Without `pcntl`/`posix` (i.e. Windows) the worker degrades to running jobs inline in the parent process: still correct, but no timeout enforcement and no signal handling.

### Reconnecting after fork

`pcntl_fork()` duplicates the parent's open sockets into the child without reopening them, so both processes share one connection — and killing the child mid-write can desync the parent's socket too. Drivers that hold a stateful connection implement `Laika\Queue\Interfaces\ReconnectableDriver`, and the worker calls `reconnect()` in the freshly forked child before running the job.

`RedisDriver` implements it (which is why `fromConfig()` matters — a driver built from a bare `Redis` instance has no way to reopen). `DatabaseDriver` and `DatabaseFailedJobProvider` don't: laika-model's `Connection` registry owns the PDO lifecycle, not the driver.

### Memory

The worker exits gracefully when memory crosses a soft threshold, letting supervisor or systemd restart it with a clean heap — this is normal operation, not a crash.

`bin/worker` first applies the framework's `CLI_MEMORY_LIMIT` (from `lf-inc/const.php`) as the process's real `memory_limit` via `Laika\Core\System\MemoryManager`. Then `Worker::work()` with `memoryLimit: null` reads PHP's *current* `memory_limit` and takes ~90% of it as the restart threshold, so the worker bows out before genuinely risking a hard OOM mid-job. Pass an explicit `int` (MB) to override.

Falls back to a flat 128MB when `laikait/laika-core` isn't installed, when `memory_limit` can't be parsed, or when it's unlimited (`-1`).

---

## Failed jobs

A job that exhausts `maxTries` — by throwing or by timing out — is handed to a `FailedJobProviderInterface` and removed from the queue, rather than silently dropped or retried forever.

```php
public function log(string $queue, string $payload, \Throwable $e): string;
public function all(?string $queue = null): array;
public function find(string $id): ?array;
public function forget(string $id): bool;
public function flush(?int $hours = null): void;
```

Two implementations: `DatabaseFailedJobProvider` and `JsonFailedJobProvider`. Chosen by `failed_driver`; there's deliberately no Redis one.

Typical workflow:

```bash
php laika queue:failed                # what broke?
php laika queue:retry <id>            # fix the cause, then re-push one
php laika queue:retry --all --queue=emails
php laika queue:flush --hours=168     # prune anything older than a week
```

Retrying is subject to the same trusted-class allow-list as the worker, since it has to unserialize the stored payload.

---

## Schema & migrations

Two tables, created only when you use a `database` driver.

**`laika_queue_jobs`** — `id` (char 36, PK), `queue` (string 100), `payload` (longtext), `attempts` (uint, default 0), `reserved_at` (uint, nullable), `available_at` (uint), `created_at` (uint), plus a composite index on `(queue, reserved_at, available_at)`.

**`laika_failed_jobs`** — `id` (char 36, PK), `queue` (string 100), `payload` (longtext), `exception` (text), `failed_at` (uint), indexed on `queue`.

There are three ways to get them:

**1. Automatic (usual).** Whenever `driver` or `failed_driver` resolves to `database`, both `bin/worker` and `QueueResolver` call `ensureSchema()` right after constructing the class. Tables appear on first run — no migrate step needed.

**2. `php laika app:migrate`.** The package declares its models and schemas under `extra.laika.resources` in its `composer.json`:

```json
"extra": {
    "laika": {
        "resources": {
            "models":  { "path": "src/Model",  "namespace": "Laika\\Queue\\Model" },
            "schemas": { "path": "src/Schema", "namespace": "Laika\\Queue\\Schema",
                         "contract": "Laika\\Model\\Contract\\SchemaAbstract" }
        }
    }
}
```

The framework's resource loader reads that from installed packages, so `app:migrate` discovers `QueueModelSchema` and `FailedJobModelSchema` alongside your own `lf-app/Schema` classes — no wiring on your side. `php laika schema:list` and `php laika resource:list` will show them.

**3. By hand.**

```php
use Laika\Queue\Schema\QueueModelSchema;
use Laika\Queue\Schema\FailedJobModelSchema;

(new QueueModelSchema('default'))->up();
(new FailedJobModelSchema('default'))->up();
```

All three are idempotent — `createIfNotExists()` underneath, safe to call on every boot.

Both schema classes extend `Laika\Model\Contract\SchemaAbstract` and use `Laika\Model\Schema\Schema` / `Blueprint`, so **schema creation needs `laikait/laika-model`**, not `laika-core`. They're plain PSR-4 files, lazily autoloaded only when something references them: constructing a driver never touches them, only `ensureSchema()` and `app:migrate` do.

The models themselves (`QueueModel`, `FailedJobModel`) are ordinary `Laika\Model\Model` subclasses with the usual `$table` / `$id` / `$connection` convention — query them directly if you want your own dashboard.

---

## Production

The worker is a long-running process, not a cron job, and it self-exits on the memory threshold. Keep it alive with a supervisor.

**supervisor**

```ini
[program:laika-queue-worker]
command=php /path/to/project/worker default
directory=/path/to/project
autostart=true
autorestart=true
numprocs=1
stopsignal=TERM
stopwaitsecs=70
user=www-data
redirect_stderr=true
stdout_logfile=/var/log/laika-queue-worker.log
```

**systemd**

```ini
[Unit]
Description=Laika queue worker
After=network.target

[Service]
ExecStart=/usr/bin/php /path/to/project/worker default
WorkingDirectory=/path/to/project
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=70
User=www-data

[Install]
WantedBy=multi-user.target
```

Set `stopwaitsecs` / `TimeoutStopSec` comfortably above your longest job timeout so a graceful stop can finish the job in flight instead of being killed.

For several queues, add one program block per queue name rather than raising `numprocs` — and read the caveat below before raising `numprocs` at all.

---

## Concurrency caveat

**`DatabaseDriver::pop()` and `JsonDriver::pop()` have no row lock.**

`DatabaseDriver` claims a job with a plain `SELECT` (filtered to unreserved, due rows) followed by an `UPDATE`, both inside a transaction, entirely through laika-model's portable query builder. There's no `FOR UPDATE` / `SKIP LOCKED` or equivalent, because those aren't portable across the seven backends laika-model targets.

The consequence: two worker processes calling `pop()` on the same queue at close to the same moment can both read the same unreserved row before either commits its `UPDATE`, and both will process that job.

This matters only if you run more than one worker on the same queue. Your options:

- **Run a single worker process per queue.** Always safe. This is the default and the right answer for most apps.
- **Make your jobs idempotent**, so double-processing is harmless.
- **Use `RedisDriver`**, whose Lua-scripted `pop()` is atomic.
- **Add a locking clause to `pop()` yourself** for your specific database.

`RedisDriver` is unaffected.

---

## Security: trusted job classes

Job payloads are restored with PHP's `unserialize()`. To prevent PHP Object Injection — an attacker-controlled payload instantiating arbitrary classes to build a gadget chain — `Job::unserializePayload()` only instantiates classes explicitly registered as trusted. The default list is **empty**, so it throws rather than silently unserializing something unexpected.

**In a Laika app this is handled for you.** `bin/worker` and `queue:retry` both register every `Job` subclass discovered under `lf-app/Job` on startup, via `Laika\Service\Infra::getQueueJobsClasses()` — the same lookup `php laika job:list` uses. Discovery only admits classes that genuinely extend `Job`, so this stays much narrower than trusting the codebase at large. No config needed.

Register manually for job classes living outside `lf-app/Job`, or when using this package standalone:

```php
use Laika\Queue\Abstracts\Job;

Job::registerTrustedClasses([
    SendWelcomeEmail::class,
    ChargeCard::class,
]);
```

Do it once at bootstrap, before any `pop()`. Calls are additive and de-duplicated.

---

## Known gaps

- **No stalled-job reaper for `DatabaseDriver`/`JsonDriver`.** A worker killed mid-job leaves `reserved_at` set forever, and that job is never picked up again. `RedisDriver::pop()` self-heals via its 90-second reserved sweep; a cron-callable `reapStalled()` for the other two is the obvious next addition.
- **`DatabaseDriver::pop()` has no locking clause** — see [Concurrency caveat](#concurrency-caveat).
- **No `QueueRelay` / `QueueServiceProvider`.** There's no facade in `Laika\Service` yet; use `Laika\Cli\QueueResolver` as shown in [Dispatching jobs](#dispatching-jobs).
- **`Job::$delay` and `Job::$queue` are not honoured by `push()`** — pass both as arguments instead. See [Two traps](#two-traps-worth-knowing).
- **No batching, chaining, or unique-job support.**

---

## Standalone use (outside the framework)

The package works without the Laika framework, with limits:

| Piece | Standalone? |
| --- | --- |
| `Job`, `Worker`, `QueueDriverInterface` | Yes — no framework dependency |
| `RedisDriver` | Yes — needs `ext-redis` only |
| `DatabaseDriver`, `DatabaseFailedJobProvider` | Yes — needs `laikait/laika-model` |
| `ensureSchema()`, the `Schema` classes | Yes — needs `laikait/laika-model` |
| `JsonDriver`, `JsonFailedJobProvider` | **No** — depends on the `APP_PATH` constant and the `setPermission()` global |
| `bin/worker`, `queue:*` / `job:*` commands | **No** — require a Laika app (`lf-boot/app.php`) |
| Auto-registered trusted classes | **No** — call `Job::registerTrustedClasses()` yourself |

A minimal standalone loop:

```php
use Laika\Model\Connection;
use Laika\Queue\Abstracts\Job;
use Laika\Queue\Driver\DatabaseDriver;
use Laika\Queue\Driver\DatabaseFailedJobProvider;
use Laika\Queue\Worker;

Connection::add([
    'driver'   => 'mysql',
    'host'     => '127.0.0.1',
    'database' => 'myapp',
    'username' => 'root',
    'password' => 'secret',
]);

Job::registerTrustedClasses([SendWelcomeEmail::class]);

$driver = new DatabaseDriver('default');
$failer = new DatabaseFailedJobProvider('default');
$driver->ensureSchema();
$failer->ensureSchema();

$driver->push(new SendWelcomeEmail($userId), 'emails', 10);

(new Worker($driver, $failer))->work('emails');
```

Note `laikait/laika-model` is **not** declared in this package's `composer.json`, since `RedisDriver` doesn't need it — install it yourself if you're using the database backend.

---

## Requirements

- PHP 8.1+
- `ext-pdo`
- [`laikait/laika-model`](https://github.com/laikait/laika-model) — for `DatabaseDriver`, `DatabaseFailedJobProvider`, and the schema classes. Not a declared dependency; comes in via `laikait/laika-core` in a framework app.
- `ext-redis` — for `RedisDriver`
- `ext-pcntl` + `ext-posix` — for signal handling and per-job timeouts (Linux/macOS). The worker degrades to inline execution without them.
- `laikait/laika-core` — only for `CLI_MEMORY_LIMIT` handling via `MemoryManager`; optional everywhere else.

## License

MIT
