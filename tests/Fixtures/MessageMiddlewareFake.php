<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Tests\Fixtures;

use KafkaBus\Core\Consumers\Pipelines\MessagePipelineHandler;
use KafkaBus\Core\Consumers\Pipelines\MessagePipelineMiddleware;
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;

final class MessageMiddlewareFake implements MessagePipelineMiddleware
{
    /**
     * @param PipelineInterface<mixed, MessagePipelineHandler> $pipeline
     * @return PipelineInterface<mixed, MessagePipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        return $pipeline->continue();
    }
}
