<?php

/**
 * Kafka Wrapper which is used to push tasks into Kafka
 * This file takes care of the conversation between PHP and Kafka
 */

namespace NirmalSharma\LaravelKafkaConsumer\Services;

use NirmalSharma\LaravelKafkaConsumer\Handlers\KafkaConsumerHandler;
abstract class Kafka {

    /**
     * Create Consumer function
     *
     * @return $consumer
     */
    public static function createConsumer($handler) {            
        $consumer = app(KafkaConsumerHandler::class);
        return $consumer->createConsumer($handler);
    }
}