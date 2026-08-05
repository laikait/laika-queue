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

    abstract public function handle(): void;

    public function failed(\Throwable $e): void
    {
        // override for custom failure logic
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
        $index = min(max(0, $this->tries - 1), count($steps) - 1);
        return $steps[$index] ?? end($steps);
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
        return unserialize($payload);
    }
}
