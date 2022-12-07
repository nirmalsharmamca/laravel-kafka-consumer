## A Lightweight Kafka Producer Warpper for Laravel 6+ and PHP 7.3+

Install Kafka Consumer Warpper

```bash
  composer require nirmalsharma/laravel-kafka-consumer
```


## Examples
[Laravel 6](examples/laravel-6-example)


## Use Kafka in code.

Add following in web.php

```bash
use Kafka;

$topic = "kafka-topic";
$data = [
    "user_ref" => "usr.123456",
    "message" => "Hello World"
];
$key = "usr.123456"; // Optional, Default null
$headers = [
    "ContentType" => "application/json",
    "Timezone" => "GMT +05:30"
]; // Optional
Kafka::push($topic, $data, $key, $headers);

```
## Environment Variables

To run this, you will need to add the following environment variables to your .env file
Config reference: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

```
IS_KAFKA_ENABLED=             // Default:  1
KAFKA_BROKERS=
KAFKA_DEBUG=                  // Default: false
KAFKA_SSL_PROTOCOL=           // Default: plaintext
KAFKA_COMPRESSION_TYPE=       // Default: none
KAFKA_IDEMPOTENCE=            // Default: false
KAFKA_CONSUMER_GROUP_ID=      // Default: group
KAFKA_OFFSET_RESET=           // Default: latest
KAFKA_AUTO_COMMIT=            // Default: true
KAFKA_ERROR_SLEEP=            // Default: 5
KAFKA_PARTITION=              // Default: 0
```


## Authors

- [Nirmal Sharma](https://github.com/nirmalsharmamca)
- [Praveen Menezes](https://github.com/praveenmenezes)


## License

[MIT](https://choosealicense.com/licenses/mit/)



## Features

- Light weight kakfa wrapper
- Easy to use event produce in code.
