<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Tests\Fixtures;

use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineHandler;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineMiddleware;

final class ProducerMiddlewareFake implements ProducerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        return $pipeline->continue();
    }
}
