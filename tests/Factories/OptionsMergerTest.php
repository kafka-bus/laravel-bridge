<?php


use KafkaBus\Laravel\Factories\OptionsMerger;

it('merge options', function () {
    $merger = new OptionsMerger([
        'additional_options' => [
            'test.option' => 'bar',
            'not.override' => 'test-value',
        ],

        'middleware' => [
            'FirstMiddlewareOnlyGlobal',
            'MiddlewareClass',
        ],

        'consume_timeout' => 5_000,
        'auto_commit' => true,
    ]);

    $result = $merger->merge([
        'additional_options' => [
            'test.option' => 'foo',
            'new.option' => 'bar',
        ],

        'middleware' => [
            'FirstMiddlewareOnlyGlobal',
            'OtherMiddlewareClass',
        ],
    ]);

    expect($result)->toEqual([
        'consume_timeout' => 5_000,
        'auto_commit' => true,

        'middleware' => [
            'MiddlewareClass',
            'FirstMiddlewareOnlyGlobal',
            'OtherMiddlewareClass',
        ],

        'additional_options' => [
            'test.option' => 'foo',
            'not.override' => 'test-value',
            'new.option' => 'bar',
        ],
    ]);
});
