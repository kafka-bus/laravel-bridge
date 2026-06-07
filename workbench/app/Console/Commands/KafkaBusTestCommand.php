<?php

namespace Workbench\App\Console\Commands;

use Illuminate\Console\Command;
use KafkaBus\Core\Interfaces\Bus\BusInterface;
use Workbench\App\Kafka\Messages\ProductDomainMessage;

final class KafkaBusTestCommand extends Command
{
    protected $signature = 'kafka-bus:test {id} {name}';
    protected $description = 'Публикация тестового сообщения';

    public function handle(BusInterface $bus): void
    {
        $message = new ProductDomainMessage([
            'id' => $this->argument('id'),
            'name' => $this->argument('name')
        ]);

        $bus->publish($message);

        $this->info('Сообщение опубликовано');
    }
}
