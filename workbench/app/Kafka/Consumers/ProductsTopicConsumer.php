<?php

namespace Workbench\App\Kafka\Consumers;

use KafkaBus\Core\Consumers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\DomainMessageFactory;
use Psr\Log\LoggerInterface;
use Workbench\App\Kafka\Messages\ProductDomainMessage;

class ProductsTopicConsumer
{
    public function __construct(
        protected LoggerInterface $logger
    ) {
    }

    #[MessageFactory(new DomainMessageFactory(ProductDomainMessage::class))]
    public function __invoke(ProductDomainMessage $message): void
    {
        $this->logger
            ->info($message->name, ['message' => $message->jsonSerialize()]);
    }
}
