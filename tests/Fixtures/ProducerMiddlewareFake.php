<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Tests\Fixtures;

use KafkaBus\Core\Pipelines\PipelineInterface;
use KafkaBus\Core\Producers\Messages\ProducerMessage;
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineHandler;
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineMiddleware;

final class ProducerMiddlewareFake implements PublisherPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerMessage, PublisherPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerMessage, PublisherPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        return $pipeline->continue();
    }
}
