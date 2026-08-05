<?php

namespace Laika\Queue\Interfaces;

interface FailedJobProviderInterface
{
    public function log(string $queue, string $payload, \Throwable $e): string;
    public function all(?string $queue = null): array;
    public function find(string $id): ?array;
    public function forget(string $id): bool;
    public function flush(?int $hours = null): void;
}
