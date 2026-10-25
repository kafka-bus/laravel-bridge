<?php

declare(strict_types=1);

use KafkaBus\Laravel\Factories\MiddlewareMerger;

it('puts global middleware first', function () {
    $merger = new MiddlewareMerger(['global']);

    expect($merger->merge(['local']))->toBe(['global', 'local']);
});

it('does not duplicate middleware present in both lists', function () {
    $merger = new MiddlewareMerger(['a', 'b']);

    expect($merger->merge(['b', 'c']))->toBe(['a', 'b', 'c']);
});

it('returns only local middleware without global ones', function () {
    expect((new MiddlewareMerger())->merge(['local']))->toBe(['local']);
});
