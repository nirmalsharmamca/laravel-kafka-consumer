<?php

/**
 * This class has been derived from the below GitHub repository
 * @link https://github.com/anam-hossain/laravel-kafka-pub-example
 */

namespace NirmalSharma\LaravelKafkaConsumer\Handlers;

use Exception;
use RdKafka\KafkaConsumer;
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
    public function __construct(KafkaConsumer $consumer) {
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

    /**
     * Set topic conf
     *
     * @return TopicConf
     */
    public function setTopicConfig() : TopicConf{
        $topicConf = new TopicConf();
        $topicConf->set('auto.commit.interval.ms', 100);

        // Set the offset store method to 'file'
        $topicConf->set('offset.store.method', 'broker');

        $topicConf->set('auto.offset.reset', config("kafka.offset_reset"));
        return $topicConf;
    }

    /**
     * Kafka Consumer
     * @param  [mixed]      $handler  Instance of class handler
     * @return void                 
     */
    public function createConsumer($handler) {
        
        $this->consumer->subscribe([config('kafka.topic')]);

        while (true) {
            $message = $this->consumer->consume(120*1000);
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



        // $topic = $this->consumer->newTopic(config('kafka.topic'), $this->setTopicConfig());
        // $partition = config('kafka.partition');
        // $topic->consumeStart($partition, RD_KAFKA_OFFSET_STORED);

        // while (true) {
            
        //     $message = $topic->consume($partition, 120*10000);

        //     if (null === $message || $message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        //         continue;
        //     } elseif ($message->err) {
        //         throw new Exception($message->errstr());
        //         break;
        //     } else {
        //         $handler($this->decodeKafkaMessage($message));
        //     }
        // }
    }

}