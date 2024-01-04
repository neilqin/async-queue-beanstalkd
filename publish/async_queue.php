<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
use Neilqin\AsyncQueueBeanstalkd\BeanstalkdDriver;

return [
    'default' => [
        'driver' => BeanstalkdDriver::class,
        'redis' => [
            'pool' => 'default',
        ],
        'bsmq' => [
            'pool' => 'default',
        ],
        'channel' => 'queue',
        'timeout' => 60,
        'retry_seconds' => 5,
        'handle_timeout' => 60,
        'processes' => 1,
        'concurrent' => [
            'limit' => 1,//beanstalkd目前只支持一次只消费一个Job，并行消费可以设置processes进程数
        ],
    ],
];
