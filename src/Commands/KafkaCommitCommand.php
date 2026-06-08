<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use Illuminate\Console\Command;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

final class KafkaCommitCommand extends Command
{
    protected $signature = 'kafka:commit {key : Commit key}';
    protected $description = 'Create a commit record in the database';

    public function handle(RepositorySourceInterface $repository): int
    {
        $key = $this->argument('key');

        $attempt = $repository->get($key);

        if ($attempt?->commitedAt !== null) {
            $this->warn("Commit for key [{$key}] already exists.");

            return self::FAILURE;
        }

        $repository->commit($key);

        $this->info("Commit for key [{$key}] has been saved.");

        return self::SUCCESS;
    }
}
