<?php

use Laika\Core\App\Resource;

// This "files"-autoload entry is required eagerly on every autoload, so it
// must stay safe to load standalone (e.g. in this package's own tests, or
// any app that hasn't installed the Laika core yet). Only register with the
// framework's resource loader when it's actually present.
if (class_exists(Resource::class)) {
    Resource::register('models', __DIR__ . '/../src/Models', 'Laika\\Queue\\Model');
    Resource::register('schemas', __DIR__ . '/../src/Schema', 'Laika\\Queue\\Schema');
}