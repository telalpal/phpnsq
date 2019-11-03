<?php

namespace OkStuff\PhpNsq\Tunnel;

class Pool
{
    private static $pool = [];

    public function __construct($nsq)
    {
        foreach ($nsq["nsq"]["nsqd-addrs"] as $value) {
            $addr = explode(":", $value);
            $this->addTunnel(new Tunnel(
                new Config($addr[0], $addr[1])
            ));
        }
    }

    public function addTunnel(Tunnel $tunnel)
    {
        static::$pool[$tunnel->getConfig()->get('host') . ':' . $tunnel->getConfig()->get('port')] = $tunnel;

        return $this;
    }

    public function getTunnel()
    {
        return static::$pool[array_rand(static::$pool)];
    }
}
