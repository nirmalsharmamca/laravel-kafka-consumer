<?php
namespace NirmalSharma\LaravelKafkaConsumer;

use Illuminate\Support\ServiceProvider;
use RdKafka\Conf;
use RdKafka\Consumer;

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

    protected function setConsumerConfig()
    {
        $conf = new Conf();

        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', 'group');

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', config("kafka.brokers"));

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $conf->set('auto.offset.reset', 'earliest');

        // Automatically and periodically commit offsets in the background
        $conf->set('enable.auto.commit', config("kafka.auto_commit"));

        return $conf;
    }

    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot() {

        $kafka_consumer_conf = $this->setConsumerConfig();
        $this->app->bind(Consumer::class, function () use ($kafka_consumer_conf) {
            return new Consumer($kafka_consumer_conf);
        });
    }
}
