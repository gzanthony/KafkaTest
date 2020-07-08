<?php

namespace App;

use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;
use Spartaques\CoreKafka\Consume\HighLevel\Contracts\Callback;

class SyncCallback implements Callback
{
    private $logger;

    public function __construct(\Monolog\Logger $logger)
    {
        $this->logger = $logger;
    }

    public function callback(\RdKafka\Message $message, ConsumerWrapper $consumer)
    {
        $this->logger->debug($message->payload);
        $consumer->commitSync();
    }
}