<?php
namespace NirmalSharma\LaravelKafkaConsumer;

use Illuminate\Support\ServiceProvider;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class KafkaConsumerServiceProvider extends ServiceProvider {
    private $config;

    /**
     * Setup the config.
     *
     * @return void
     */
    protected function setupConfig(): void{
        $source = realpath($raw = __DIR__ . '/../config/kafka.php') ?: $raw;
        $this->mergeConfigFrom($source, 'kafka');
    }

    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot() {
        $this->setupConfig();
    }
}
