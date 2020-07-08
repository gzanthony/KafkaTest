<?php
require "./vendor/autoload.php";

use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Common\DefaultCallbacks;
use Spartaques\CoreKafka\Produce\ProducerWrapper;
use Spartaques\CoreKafka\Produce\ProducerData;
use Spartaques\CoreKafka\Produce\ProducerProperties;

$logger = new \Monolog\Logger("general");
$logger->pushHandler(new \Monolog\Handler\StreamHandler("php://stdout", \Monolog\Logger::DEBUG));

$producer = new ProducerWrapper();

$callbacksInstance = new DefaultCallbacks();

$collection = new CallbacksCollection(
    [
        ConfigurationCallbacksKeys::DELIVERY_REPORT => $callbacksInstance->delivery(),
        ConfigurationCallbacksKeys::ERROR => $callbacksInstance->error(),
        ConfigurationCallbacksKeys::LOG => $callbacksInstance->log(),
    ]);

// producer initialization object
$produceData = new ProducerProperties('test123', ['metadata.broker.list' => '192.168.2.151:9092', 'client.id' => 'test'], ['partitioner' => 'consistent'], $collection);

$i = 0;
while (true) {
    $producer->init($produceData)->produce(new ProducerData("Message $i", 0, 0, $i), 100);
    $producer->flush();
    sleep(1);
    $i++;
}
