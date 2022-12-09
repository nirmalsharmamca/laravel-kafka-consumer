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
     * Setup configs for Kafka Consumer
     *
     * @return \RdKafka\Conf
     */
    protected function setConsumerConfig(): Conf{
        $conf = new Conf();

        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', config("kafka.consumer_group_id"));

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', config("kafka.brokers"));

        // SSL Protocol
        $conf->set('security.protocol', config("kafka.ssl_protocol"));

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $conf->set('auto.offset.reset', config("kafka.offset_reset"));

        // Automatically and periodically commit offsets in the background
        $conf->set('enable.auto.commit', config("kafka.auto_commit"));

        // // Set a rebalance callback to log partition assignments (optional)
        // $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
        //     switch ($err) {
        //         case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        //             $kafka->assign($partitions);
        //             break;

        //         case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        //             $kafka->assign(NULL);
        //             break;

        //         default:
        //             throw new \Exception($err);
        //     }
        // });

        return $conf;
    }

    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot() {
        $this->setupConfig();

        $kafka_consumer_conf = $this->setConsumerConfig();
        $this->app->bind(KafkaConsumer::class, function () use ($kafka_consumer_conf) {
            return new KafkaConsumer($kafka_consumer_conf);
        });
    }
}
