<?php

/**
 * This class has been derived from the below GitHub repository
 * @link https://github.com/anam-hossain/laravel-kafka-pub-example
 */

namespace NirmalSharma\LaravelKafkaConsumer\Handlers;

use Exception;
use RdKafka\Consumer;
use RdKafka\Message;
use RdKafka\TopicConf;
use Log;

class KafkaConsumerHandler {
    /**
     * Topic missing error message
     */
    const TOPIC_MISSING_ERROR_MESSAGE = 'Topic is not set';

    /**
     * Flush error message
     */
    const FLUSH_ERROR_MESSAGE = 'librdkafka unable to perform flush, messages might be lost';

    /**
     * Message payload
     *
     * @var string
     */
    protected $payload;

    /**
     * Kafka topic
     *
     * @var string
     */
    protected $topic;

    /**
     * RdKafka producer
     *
     * @var \RdKafka\Producer
     */
    protected $consumer;

    /**
     * Kafka message
     *
     * @var array
     */
    private $message;

    /**
     * Kafka key
     *
     * @var string
     */
    private $key;

    /**
     * KafkaConsumer's constructor
     *
     * @param \RdKafka\KafkaConsumer $producer
     */
    public function __construct(Consumer $consumer) {
        $this->consumer = $consumer;
    }

    /**
     * Set kafka topic
     *
     * @param  string  $topic
     * @return $this
     */
    public function setTopic(string $topic) {
        $this->topic = $topic;

        return $this;
    }

    /**
     * Get topic
     *
     * @return string
     */
    public function getTopic() {
        if (!$this->topic) {
            throw new Exception(self::TOPIC_MISSING_ERROR_MESSAGE);
        }

        return $this->topic;
    }

    /**
     * Set Message Payload
     *
     * @param  array  $data Message Data
     * @return void
     */
    public function setMessage(array $data) {
        $this->message = json_encode($data);
    }

    /**
     * Set Kafka Key
     *
     * @param  string $key Key
     * @return void
     */
    public function setKey(string $key) {
        $this->key = $key;
    }

    /**
     * Decode kafka message
     *
     * @param  \RdKafka\Message $kafkaMessage
     * @return object
     */
    public function decodeKafkaMessage(Message $kafkaMessage) {
        $message = json_decode($kafkaMessage->payload, true);

        if (isset($message->body) && is_string($message->body)) {
            $message->body = json_decode($message->body, true);
        }

        return $message;
    }

    public function setTopicConfig() {
        $topicConf = new TopicConf();
        $topicConf->set('auto.commit.interval.ms', 100);

        // Set the offset store method to 'file'
        $topicConf->set('offset.store.method', 'broker');

        $topicConf->set('auto.offset.reset', 'earliest');
        return $topicConf;
    }

    /**
     * Kafka Consumer
     * @param  string      $topic     Indicates Kafka Topic
     * @param  int|integer $partition Indicates Topic partition
     * @param  [type]      $handler   [description]
     * @return [type]                 [description]
     */
    public function createConsumer(string $topic, int $partition = 0, $handler) {
        $topic = $this->consumer->newTopic($topic, $this->setTopicConfig());
        $topic->consumeStart($partition, RD_KAFKA_OFFSET_STORED);

        while (true) {
            
            $message = $topic->consume($partition, 120*10000);

            if (null === $message || $message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                continue;
            } elseif ($message->err) {
                throw new Exception($message->errstr());
                break;
            } else {
                $handler($this->decodeKafkaMessage($message));
            }
        }
    }

}