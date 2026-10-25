<?php

namespace KafkaBus\Laravel\Factories;

final readonly class OptionsMerger
{
    private MiddlewareMerger $middlewareMerger;

    public function __construct(
        private array $globalOptions
    ) {
        $this->middlewareMerger = new MiddlewareMerger($this->globalOptions['middleware'] ?? []);
    }

    public function merge(array $options): array
    {

        return [
           ...$this->globalOptions,
           ...$options,

           'middleware' => $this->middlewareMerger->merge($options['middleware'] ?? []),

           'additional_options' => [
               ...($this->globalOptions['additional_options'] ?? []),
               ...($options['additional_options'] ?? []),
           ],
        ];
    }
}
