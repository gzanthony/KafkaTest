<?php

namespace App;

use Monolog\Handler\RotatingFileHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Common\DefaultCallbacks;
use Spartaques\CoreKafka\Produce\ProducerWrapper;
use Spartaques\CoreKafka\Produce\ProducerData;
use Spartaques\CoreKafka\Produce\ProducerProperties;

class Producer extends Command
{
    protected static $defaultName = "ka:producer";

    public function configure()
    {
        $this->setDescription("Kafka producer.")
            ->addArgument("topic", InputArgument::REQUIRED, "topic")
            ->addOption("level", "ll", InputOption::VALUE_OPTIONAL, "log level", "info")
            ->addOption("stdout", "lg", InputOption::VALUE_NONE, "log stdout")
            ->addOption("kafka_host", "host", InputOption::VALUE_OPTIONAL, "kafka host", "192.168.2.151:9092")
            ->addOption("kafka_client_id", "client_id", InputOption::VALUE_OPTIONAL, "kafka client id", "cli_" . time())
            ->addOption("kafka_partition", "partition", InputOption::VALUE_OPTIONAL, "kafka topic partition", null)
            ->addOption("duration", "du", InputOption::VALUE_OPTIONAL, "sleep time, support float, like: 1.5", 1);
    }

    public function execute(InputInterface $input, OutputInterface $output)
    {
        $logger = new Logger("general");
        $topic = $input->getArgument("topic");
        $kafka_client = $input->getOption("kafka_client_id");
        $kafka_host = $input->getOption("kafka_host");
        $kafka_partition = $input->getOption("kafka_partition");
        $duration = $input->getOption("duration") * 1000000;

        $log_level = Logger::INFO;

        switch ($input->getOption("level")) {
            case "debug":
                $log_level = Logger::DEBUG;
                break;
            case "error":
                $log_level = Logger::ERROR;
                break;
            case "alert":
                $log_level = Logger::ALERT;
                break;
        }

        if ($input->getOption("stdout")) {
            $logger->pushHandler(new StreamHandler("php://stdout", $log_level));
        } else {
            if (!file_exists(ROOT_PATH . '/log')) @mkdir(ROOT_PATH . "/log", 0755, true);
            $logger->pushHandler(new RotatingFileHandler(ROOT_PATH . '/log/product_' . $topic . '.log', 5, $log_level));
        }

        $producer = new ProducerWrapper();
        $callbacksInstance = new DefaultCallbacks();
        $collection = new CallbacksCollection(
            [
                ConfigurationCallbacksKeys::DELIVERY_REPORT => $callbacksInstance->delivery(),
                ConfigurationCallbacksKeys::ERROR => $callbacksInstance->error(),
                ConfigurationCallbacksKeys::LOG => $callbacksInstance->log(),
            ]);
        // producer initialization object
        $produceData = new ProducerProperties($topic, ['metadata.broker.list' => $kafka_host, 'client.id' => $kafka_client], ['partitioner' => 'consistent'], $collection);

        $i = 0;
        while (true) {
            $producer->init($produceData)->produce(new ProducerData("Message $i", RD_KAFKA_PARTITION_UA, 0, $i), 100);
            $producer->flush();
            usleep($duration);
            $i++;
        }

        return 1;
    }
}