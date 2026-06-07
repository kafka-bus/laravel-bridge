<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Tests\Fixtures;

use KafkaBus\Core\Consumers\Pipelines\MessagePipelineHandler;
use KafkaBus\Core\Consumers\Router\MessagePipelineMiddleware;
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;

final class MessageMiddlewareFake implements MessagePipelineMiddleware
{
    /**
     * @param PipelineInterface<MessagePipelineHandler> $pipeline
     * @return PipelineInterface<MessagePipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        return $pipeline->continue();
    }
}
