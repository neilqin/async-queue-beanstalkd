<?php


namespace Neilqin\AsyncQueueBeanstalkd;


use Hyperf\AsyncQueue\Driver\ChannelConfig;
use Hyperf\AsyncQueue\Driver\Driver;
use Hyperf\AsyncQueue\Event\AfterHandle;
use Hyperf\AsyncQueue\Event\BeforeHandle;
use Hyperf\AsyncQueue\Event\FailedHandle;
use Hyperf\AsyncQueue\Event\RetryHandle;
use Hyperf\AsyncQueue\Exception\InvalidQueueException;
use Hyperf\AsyncQueue\JobInterface;
use Hyperf\AsyncQueue\JobMessage;
use Hyperf\AsyncQueue\MessageInterface;
use Hyperf\Collection\Arr;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Neilqin\Bsmq\BsmqFactory;
use Neilqin\Bsmq\BsmqProxy;
use Pheanstalk\Job as PheanstalkJob;
use Psr\Container\ContainerInterface;
use Hyperf\Process\ProcessManager;
use Throwable;

use function Hyperf\Support\make;

class BeanstalkdDriver extends Driver
{
    protected RedisProxy $redis;
    protected BsmqProxy $bsmq;

    protected ChannelConfig $channel;

    /**
     * Max polling time.
     */
    protected int $timeout;

    /**
     * Retry delay time.
     */
    protected array|int $retrySeconds;

    /**
     * Handle timeout.
     */
    protected int $handleTimeout;

    /**
     *
     * @var int
     */
    protected int $priority = 1024;

    /**
     * @var StdoutLoggerInterface
     */
    protected $logger = null;

    public function __construct(ContainerInterface $container, $config)
    {
        parent::__construct($container, $config);
        $channel = $config['channel'] ?? 'queue';
        $this->redis = $container->get(RedisFactory::class)->get($config['redis']['pool'] ?? 'default');
        $this->bsmq = $container->get(BsmqFactory::class)->get($config['bsmq']['pool'] ?? 'default');
        $this->timeout = $config['timeout'] ?? 5;
        $this->retrySeconds = $config['retry_seconds'] ?? 10;
        $this->handleTimeout = $config['handle_timeout'] ?? 10;

        $this->channel = make(ChannelConfig::class, ['channel' => $channel]);

        $this->logger = $container->get(StdoutLoggerInterface::class);
    }

    public function push(JobInterface $job, int $delay = 0): bool
    {
        $message = make(JobMessage::class, [$job]);
        $data = $this->packer->pack($message);
        return (bool) $this->bsmq->putInTube($this->channel->getChannel(), $data, $this->priority, $delay, $this->timeout);
    }

    public function delete(JobInterface $job): bool
    {
        throw new \Exception('async queue base on beanstalkd not call delete function,use remove function');
        $message = make(JobMessage::class, [$job]);
        $data = $this->packer->pack($message);

        return (bool) $this->redis->zRem($this->channel->getDelayed(), $data);
    }

    public function pop(): array
    {
        $job = $this->bsmq->watchOnly($this->channel->getChannel())->reserve($this->timeout);
//        $job = $this->bsmq->reserveFromTube($this->channel->getChannel(), $this->timeout);
        if ($job instanceof PheanstalkJob) {
            $data = $job->getData();
            $message = $this->packer->unpack($data);
            if (! $message) {
                return [false, null];
            }
            return [$job, $message];
        }
        else {
            return [false, null];
        }
    }

    public function ack(mixed $data): bool
    {
        return $this->remove($data);
    }

    public function fail(mixed $data): bool
    {
        return (bool) $this->redis->lPush($this->channel->getFailed(), (string) $data->getData());
    }

    public function reload(string $queue = null): int
    {
        $channel = $this->channel->getFailed();
        if ($queue) {
            if (! in_array($queue, ['timeout', 'failed'])) {
                throw new InvalidQueueException(sprintf('Queue %s is not supported.', $queue));
            }

            $channel = $this->channel->get($queue);
        }

        $num = 0;
        while ($data = $this->redis->rpop($channel)) {
            $this->bsmq->putInTube($this->channel->getChannel(), $data, $this->priority, 0, $this->timeout);
            ++$num;
        }
        return $num;
    }

    public function flush(string $queue = null): bool
    {
        $channel = $this->channel->getFailed();
        if ($queue) {
            $channel = $this->channel->get($queue);
        }
        return (bool) $this->redis->del($channel);
    }

    public function info(): array
    {
        $status = $this->bsmq->statsTube($this->channel->getChannel());
        $statusArr = $status->getArrayCopy();
        return [
            'waiting' => intval($statusArr['current-jobs-ready']),
            'delayed' => intval($statusArr['current-jobs-delayed']),
            'reserved' => intval($statusArr['current-jobs-reserved']),
            'failed' => $this->redis->lLen($this->channel->getFailed()),
            'timeout' => $this->redis->lLen($this->channel->getTimeout()),
        ];
    }

    protected function retry(MessageInterface $message): bool
    {
        $data = $this->packer->pack($message);
        $delay = $this->getRetrySeconds($message->getAttempts());
        $id = $this->bsmq->putInTube($this->channel->getChannel(), $data, $this->priority, $delay, $this->timeout);
        return (bool) $id;
    }

    protected function getRetrySeconds(int $attempts): int
    {
        if (! is_array($this->retrySeconds)) {
            return $this->retrySeconds;
        }

        if (empty($this->retrySeconds)) {
            return 10;
        }

        return $this->retrySeconds[$attempts - 1] ?? end($this->retrySeconds);
    }

    /**
     * Remove data from reserved queue.
     */
    protected function remove(mixed $data): bool
    {
        if ($data instanceof PheanstalkJob) {
            $flag = $this->bsmq->useTube($this->channel->getChannel())->delete($data);
            return (bool) $flag;
        }

        return false;
    }

    public function consume(): void
    {
//        ini_set('default_socket_timeout', 86400*7);
        $messageCount = 0;
        $maxMessages = Arr::get($this->config, 'max_messages', 0);
        $bsmpTmp = make(BsmqProxy::class, ['pool' => ($this->config['bsmq']['pool'] ?? 'default')]);
        $beanstalkd = $bsmpTmp->getConnection()->getConnection();//BsqmConnection
        while (ProcessManager::isRunning()) {
            try {
                $message = null;
                $job = $beanstalkd->watchOnly($this->channel->getChannel())->reserve($this->timeout);
                if ($job instanceof PheanstalkJob) {
                    $data = $job->getData();
                    $message = $this->packer->unpack($data);
                }
                if (!$message) {
                    continue;
                }
                try {
                    if ($message instanceof MessageInterface) {
                        $this->event?->dispatch(new BeforeHandle($message));
                        $message->job()->handle();
                        $this->event?->dispatch(new AfterHandle($message));
                    }
                    $beanstalkd->delete($job);
                }
                catch (Throwable $ex) {
                    $exMessage = $ex->getMessage();
                    $this->logger->error("beanstalkd consume error:". $ex->getMessage());
                    if (stripos($exMessage, 'Cannot delete job') === false) {//不是删除队列错误时执行下面程序
                        if (isset($message, $job)) {
                            $this->logger->warning("beanstalkd consume error, queue while be retry or fail");
                            $isdeleted = false;
                            try {
                                $beanstalkd->delete($job);
                                $isdeleted = true;
                            }
                            catch (\exception $exDelete) {
                            }

                            if ($message->attempts() && $isdeleted) {
                                $this->event?->dispatch(new RetryHandle($message, $ex));
                                $this->retry($message);
                            } else {
                                $this->event?->dispatch(new FailedHandle($message, $ex));
                                $this->fail($job);
                            }
                        }
                    }
                }
                if ($messageCount % $this->lengthCheckCount === 0) {
                    $this->checkQueueLength();
                }
                if ($maxMessages > 0 && $messageCount >= $maxMessages) {
                    break;
                }
            } catch (Throwable $exception) {
                $this->logger->error((string) $exception);
            } finally {
                ++$messageCount;
            }
        }
    }
}
