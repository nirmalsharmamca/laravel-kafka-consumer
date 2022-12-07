<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Kafka;

class KafkaController extends Controller {
    
    public function run(Request $request) {
        $topic = "kafka-topic";
        $data = [
            "user_ref" => "usr.123456",
            "message" => "Hello World"
        ];
        $key = "usr.123456";
        $headers = [
            "ContentType" => "application/json",
            "Timezone" => "GMT +05:30"
        ];
        Kafka::push($topic, $data, $key, $headers);
    }
 }
