<?php

namespace KafkaBus\Laravel\Factories;

final readonly class OptionsMerger
{
    public function __construct(
        private array $globalOptions
    ) {
    }

    public function merge(array $options): array
    {
        $middlewares = $options['middleware'] ?? [];
        $globalMiddleware = array_diff($this->globalOptions['middleware'] ?? [], $middlewares);

        return [
           ...$this->globalOptions,
           ...$options,

           'middleware' => [
               ...$globalMiddleware,
               ...$middlewares,
           ],

           'additional_options' => [
               ...($this->globalOptions['additional_options'] ?? []),
               ...($options['additional_options'] ?? []),
           ],
        ];
    }
}
