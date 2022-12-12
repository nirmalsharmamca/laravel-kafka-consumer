<?php

/**
 * This class has been derived from the below GitHub repository
 * @link https://github.com/anam-hossain/laravel-kafka-pub-example
 */

namespace NirmalSharma\LaravelKafkaConsumer\Handlers;

use Exception;
use Log;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\TopicPartition;

class KafkaConsumerHandler {
    /**
     * Topic missing error message
     */
    const TOPIC_MISSING_ERROR_MESSAGE = 'Topic is not set';

    /**
     * Kafka Consumer Configuration
     *
     * @var \RdKafka\Conf
     */
    protected $conf;

    /**
     * Kafka Consumer Instance
     *
     * @var \RdKafka\KafkaConsumer
     */
    protected $consumer;

    public function __construct(Conf $conf) {
        $this->setConsumerConfig($conf);
    }

    /**
     * Setup configs for Kafka Consumer
     *
     * @return \RdKafka\Conf
     */
    protected function setConsumerConfig(Conf $conf) {

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

        // Emit EOF event when reaching the end of a partition
        $conf->set('enable.partition.eof', 'true');

        // Automatically and periodically commit offsets in the background
        $conf->set('enable.auto.commit', config("kafka.auto_commit"));

        $this->conf = $conf;
    }

    /**
     * Decode kafka message
     *
     * {
     *     "headers"    => "message-headers",
     *     "key"        => "message-key",
     *     "body"       => "message-body"
     * }
     * @param  \RdKafka\Message $kafka_message
     * @return object
     */
    public function decodeKafkaMessage(Message $kafka_message) {
        $message = json_decode($kafka_message->payload, true);

        if (isset($message->body) && is_string($message->body)) {
            $message->body = json_decode($message->body, true);
        }

        return $message;
    }

    /**
     * Kafka Consumer
     * @param  mixed  $handler Instance of Consumer Handler
     * @return void
     */
    public function createConsumer($handler) {
        $this->consumer = new KafkaConsumer($this->conf);

        # Setup Kafka parition
        $partition = config('kafka.partition');
        if ($partition != null) {
            $this->consumer->assign([
                new TopicPartition(config('kafka.topic'), $partition),
            ]);
        } else {
            $this->consumer->subscribe([config('kafka.topic')]);
        }

        # Run the Consumer
        while (true) {
            $message = $this->consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $handler($this->decodeKafkaMessage($message));
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    Log::debug("Error", ["No more messages; will wait for more"]);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    Log::debug("Error", ["Timed out"]);
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

}