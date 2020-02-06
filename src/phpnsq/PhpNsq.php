<?php

namespace OkStuff\PhpNsq;

use Closure;
use Exception;
use OkStuff\PhpNsq\Command\Base as SubscribeCommand;
use OkStuff\PhpNsq\Tunnel\Pool;
use OkStuff\PhpNsq\Tunnel\Tunnel;
use OkStuff\PhpNsq\Wire\Reader;
use OkStuff\PhpNsq\Wire\Writer;

class PhpNsq
{
    private $pool;
    private $channel;
    private $topic;
    private $reader;

    const LOG_ERROR = 'ERROR';
    const LOG_INFO = 'INFO';

    public function __construct($nsq)
    {
        $this->reader = new reader();
        $this->pool   = new Pool($nsq);
    }

    private function log($message, $exception=null, $level=self::LOG_ERROR)
    {
        error_log("PHPNSQ.$level $message:");
        if ($exception)
        {
            error_log($exception);
        }
    }

    public function setChannel($channel)
    {
        $this->channel = $channel;

        return $this;
    }

    public function setTopic($topic)
    {
        $this->topic = $topic;

        return $this;
    }

    public function publish($message)
    {
        try {
            $tunnel = $this->pool->getTunnel();
            $tunnel->write(Writer::pub($this->topic, $message));
        } catch (Exception $e) {
            $this->log('publish error', $e);
        }
    }

    public function publishMulti(...$bodies)
    {
        try {
            $tunnel = $this->pool->getTunnel();
            $tunnel->write(Writer::mpub($this->topic, $bodies));
        } catch (Exception $e) {
            $this->log('publish error', $e);
        }
    }

    public function publishDefer($message, $deferTime)
    {
        try {
            $tunnel = $this->pool->getTunnel();
            $tunnel->write(Writer::dpub($this->topic, $deferTime, $message));
        } catch (Exception $e) {
            $this->log('publish error', $e);
        }
    }

    public function subscribe(SubscribeCommand $cmd, Closure $callback)
    {
        try {
            $tunnel = $this->pool->getTunnel();
            $sock   = $tunnel->getSock();

            $cmd->addReadStream($sock, function ($sock) use ($tunnel, $callback) {
                $this->handleMessage($tunnel, $callback);
            });

            $tunnel->write(Writer::sub($this->topic, $this->channel))->write(Writer::rdy(1));
        } catch (Exception $e) {
            $this->log('subscribe error', $e);
        }
    }

    protected function handleMessage(Tunnel $tunnel, $callback)
    {
        $reader = $this->reader->bindTunnel($tunnel)->bindFrame();

        if ($reader->isHeartbeat()) {
            $tunnel->write(Writer::nop());
        } elseif ($reader->isMessage()) {

            $msg = $reader->getMessage();
            try {
                call_user_func($callback, $msg);
            } catch (Exception $e) {
                $this->log('Will be requeued', $e->getMessage());

                $tunnel->write(Writer::touch($msg->getId()))
                    ->write(Writer::req(
                        $msg->getId(),
                        $tunnel->getConfig()->get("defaultRequeueDelay")["default"]
                    ));
            }

            $tunnel->write(Writer::fin($msg->getId()))
                ->write(Writer::rdy(1));
        } elseif ($reader->isOk()) {
            $this->log('Ignoring "OK" frame in SUB loop', null, self::LOG_INFO);
        } else {
            $this->log("Error/unexpected frame received", $reader);
        }
    }
}
