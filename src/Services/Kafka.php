<?php

/**
 * Kafka Wrapper which is used to push tasks into Kafka
 * This file takes care of the conversation between PHP and Kafka
 */

namespace NirmalSharma\LaravelKafkaProducer\Services;

use NirmalSharma\LaravelKafkaProducer\Handlers\KafkaConsumerHandler;

abstract class Kafka {

    /**
     * Create Consumer function
     *
     * @param string $topic
     * @return $consumer
     */
    public static function createConsumer(string $topic, int $partition = 0, $handler) {            
        $obj = app(KafkaConsumerHandler::class);
        return $obj->createConsumer($topic, $partition, $handler);
    }
}