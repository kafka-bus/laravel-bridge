<?php

declare(strict_types=1);

namespace KafkaBus\Laravel\Commands;

use DateTimeImmutable;
use Illuminate\Console\Command;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;
use KafkaBus\Laravel\Components\Commiter\PurgeInterface;

final class KafkaCommitPurgeCommand extends Command
{
    protected $signature = 'kafka:commit:purge {--days= : Number of days to retain (overrides config)}';
    protected $description = 'Delete committed records older than the configured retention period';

    public function handle(RepositorySourceInterface $source): int
    {
        if (! $source instanceof PurgeInterface) {
            $this->error('The active repository source does not support purging.');

            return self::FAILURE;
        }

        $days = (int) ($this->option('days') ?? config('kafka-bus-commiter.purge.days', 14));
        $before = new DateTimeImmutable("-{$days} days");

        $count = $source->purge($before);

        $this->info("Purged {$count} commit(s) older than {$days} day(s).");

        return self::SUCCESS;
    }
}
