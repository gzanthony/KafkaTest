<?php
require './vendor/autoload.php';
require './SyncCallback.php';

use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Common\DefaultCallbacks;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerProperties;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;

$logger = new \Monolog\Logger("general");
$logger->pushHandler(new \Monolog\Handler\StreamHandler("php://stdout", \Monolog\Logger::DEBUG));

$callbacksInstance = new DefaultCallbacks();

$collection = new CallbacksCollection(
    [
        ConfigurationCallbacksKeys::CONSUME => $callbacksInstance->consume(),
        ConfigurationCallbacksKeys::DELIVERY_REPORT => $callbacksInstance->delivery(),
        ConfigurationCallbacksKeys::ERROR => $callbacksInstance->error(),
        ConfigurationCallbacksKeys::LOG => $callbacksInstance->log(),
        ConfigurationCallbacksKeys::OFFSET_COMMIT => $callbacksInstance->commit(),
        ConfigurationCallbacksKeys::REBALANCE => $callbacksInstance->rebalance(),
        ConfigurationCallbacksKeys::STATISTICS => $callbacksInstance->statistics(),
    ]
);

$consumer = new ConsumerWrapper();

$consumeDataObject = new ConsumerProperties(
    [
        'group.id' => 'test1',
        'client.id' => 'test',
        'metadata.broker.list' => '192.168.2.151:9092',
        'auto.offset.reset' => 'smallest',
        'enable.auto.commit' => "false",
//        'auto.commit.interval.ms' => 0
    ],
    $collection
);


$consumer->init($consumeDataObject)->consume(['test123'], new SyncCallback($logger));
