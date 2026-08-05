<?php

namespace Laika\Queue;

use Laika\Core\Relay\Relay;

/**
 * NOTE: method name getRelayAccessor() and base namespace Laika\Core\Relay\Relay
 * are assumptions based on partial framework details — adjust to match your
 * actual Relay base class signature.
 */
class QueueRelay extends Relay
{
    protected static function getRelayAccessor(): string
    {
        return 'queue';
    }
}
