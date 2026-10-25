<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Factories;

final readonly class MiddlewareMerger
{
    /**
     * @param list<class-string> $globalMiddleware
     */
    public function __construct(
        private array $globalMiddleware = []
    ) {
    }

    /**
     * @param list<class-string> $middleware
     * @return list<class-string>
     */
    public function merge(array $middleware): array
    {
        $globalMiddleware = array_diff($this->globalMiddleware, $middleware);

        return [
            ...$globalMiddleware,
            ...$middleware,
        ];
    }
}
