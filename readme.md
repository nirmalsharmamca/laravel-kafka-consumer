## A Lightweight Kafka Producer Warpper for Laravel 6+ and PHP 7.3+

Install Kafka Consumer Warpper

```bash
  composer require nirmalsharma/laravel-kafka-consumer
```


## Examples
[Laravel 6](examples/laravel-6-example)


## Use Kafka in code.

Sample code for console

```bash
namespace App\Console\Commands;

use App\Handlers\TestHandler;
use Illuminate\Console\Command;
use NirmalSharma\LaravelKafkaConsumer\Services\Kafka;

class TestTopicConsumer extends Command
{
    protected $signature = 'kafka:test-consume';

    protected $description = 'Command description';

    public function handle(): void
    {
      Kafka::createConsumer('ring-test-topic', 0, new TestHandler);
    }
}

TestHandler.php
-----------------

namespace App\Handlers;

use Illuminate\Support\Facades\Log;

class TestHandler
{
    public function __invoke( $message)
    {
        Log::debug('Message received!', [
            $message
        ]);
    }
}

```

## To start listening messages by run following command: 

```
  php artisan kafka:test-consume
```

## You can check handler log message in "storage/logs/laravel.log" file or run the following command in your terminal:
```
  tail -f storage/logs/laravel.log
```

## Environment Variables

To run this, you will need to add the following environment variables to your .env file
Config reference: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

```
IS_KAFKA_ENABLED=             // Default:  1
KAFKA_BROKERS=
KAFKA_DEBUG=                  // Default: false
KAFKA_SSL_PROTOCOL=           // Default: plaintext or ssl for consumer
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
