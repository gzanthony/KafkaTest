<?php
require __DIR__ . '/vendor/autoload.php';

define("ROOT_PATH", __DIR__);

use Symfony\Component\Console\Application;

$app = new Application();
$app->setName("Kafka test console");
$app->setVersion('0.0.1');

// add command
$app->add(new \App\Producer());
$app->add(new \App\Consumer());

$app->run();
