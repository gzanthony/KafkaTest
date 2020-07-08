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
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerProperties;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;

class Consumer extends Command
{
    protected static $defaultName = "ka:consume";

    protected function configure()
    {
        $this->setDescription("Kafka producer.")
            ->addArgument("topic", InputArgument::REQUIRED, "topic")
            ->addOption("level", "ll", InputOption::VALUE_OPTIONAL, "log level", "info")
            ->addOption("stdout", "lg", InputOption::VALUE_NONE, "log stdout")
            ->addOption("kafka_host", "host", InputOption::VALUE_OPTIONAL, "kafka host", "192.168.2.151:9092")
            ->addOption("kafka_client_id", "client_id", InputOption::VALUE_OPTIONAL, "kafka client id", "cli_" . time())
            ->addOption("kafka_partition", "partition", InputOption::VALUE_OPTIONAL, "kafka topic partition", 0)
            ->addOption("kafka_group", "group_id", InputOption::VALUE_REQUIRED, "kafka group id", null);
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $topic = $input->getArgument("topic");
        $kafka_client = $input->getOption("kafka_client_id");
        $kafka_host = $input->getOption("kafka_host");
        $kafka_partition = $input->getOption("kafka_partition");
        $kafka_group = $input->getOption("kafka_group");

        $logger = new Logger($topic);
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
            $logger->pushHandler(new StreamHandler("php://stdout", Logger::DEBUG));
        } else {
            if (!file_exists(ROOT_PATH . '/log')) @mkdir(ROOT_PATH . "/log", 0755, true);
            $logger->pushHandler(new RotatingFileHandler(ROOT_PATH . '/log/product_' . $topic . '.log', 5, $log_level));
        }

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
                'group.id' => $kafka_group,
                'client.id' => $kafka_client,
                'metadata.broker.list' => $kafka_host,
                'auto.offset.reset' => 'smallest',
                'enable.auto.commit' => "false",
            ],
            $collection
        );

        if (!empty($kafka_partition)) {
            $consumer->init($consumeDataObject)->assign([new \RdKafka\TopicPartition($topic, $kafka_partition)])->consumeWithManualAssign(new \App\SyncCallback($logger));
        } else {
            $consumer->init($consumeDataObject)->consume([$topic], new \App\SyncCallback($logger));
        }
    }
}