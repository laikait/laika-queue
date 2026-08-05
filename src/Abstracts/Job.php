<?php

namespace Laika\Queue\Abstracts;

abstract class Job
{
    public string $id = '';
    public string $queue = 'default';
    public int $tries = 0;
    public int $maxTries = 3;
    public int $retryAfter = 60;
    public int $delay = 0;

    /** Fixed seconds, per-attempt array, or null = exponential */
    protected int|array|null $backoffStrategy = null;
    protected bool $jitter = true;

    /**
     * Class names unserializePayload() is permitted to instantiate. Empty by
     * default, which means unserialize() rejects every object (safe default)
     * rather than allowing arbitrary classes — the classic PHP Object
     * Injection gadget-chain vector. Register your own Job subclasses once at
     * bootstrap:
     *
     *   Job::registerTrustedClasses([SendWelcomeEmail::class, ...]);
     */
    protected static array $trustedClasses = [];

    abstract public function handle(): void;

    public function failed(\Throwable $e): void
    {
        // override for custom failure logic
    }

    public static function registerTrustedClasses(array $classes): void
    {
        self::$trustedClasses = array_values(array_unique([...self::$trustedClasses, ...$classes]));
    }

    public function backoff(): int
    {
        $seconds = match (true) {
            is_array($this->backoffStrategy) => $this->backoffFromArray($this->backoffStrategy),
            is_int($this->backoffStrategy) => $this->backoffStrategy * max(1, $this->tries),
            default => $this->exponentialBackoff(),
        };

        return $this->jitter ? $this->applyJitter($seconds) : $seconds;
    }

    protected function backoffFromArray(array $steps): int
    {
        if ($steps === []) {
            return $this->exponentialBackoff();
        }

        $index = min(max(0, $this->tries - 1), count($steps) - 1);
        return $steps[$index];
    }

    protected function exponentialBackoff(int $cap = 3600): int
    {
        $seconds = $this->retryAfter * (2 ** max(0, $this->tries - 1));
        return min($seconds, $cap);
    }

    protected function applyJitter(int $seconds, float $factor = 0.2): int
    {
        $spread = (int) round($seconds * $factor);
        return $spread > 0 ? $seconds + random_int(-$spread, $spread) : $seconds;
    }

    public function serializePayload(): string
    {
        return serialize($this);
    }

    public static function unserializePayload(string $payload): self
    {
        $job = unserialize($payload, ['allowed_classes' => self::$trustedClasses]);

        if (!$job instanceof self) {
            throw new \RuntimeException(
                'Failed to unserialize job payload: its class is not registered as trusted. ' .
                'Call Job::registerTrustedClasses() with your Job subclasses at bootstrap.'
            );
        }

        return $job;
    }
}
