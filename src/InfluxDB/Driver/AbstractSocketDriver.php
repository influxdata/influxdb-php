<?php
/**
 * @author Stephen "TheCodeAssassin" Hoogendijk
 */

namespace InfluxDB\Driver;

/**
 * Class UDP
 *
 * @package InfluxDB\Driver
 */
abstract class AbstractSocketDriver implements DriverInterface
{
    /**
     * Parameters
     *
     * @var array
     */
    private $parameters;

    /**
     * @var array
     */
    protected $config;

    /**
     *
     * @var resource
     */
    protected $stream;

    /**
     * @param string $host IP/hostname of the InfluxDB host
     * @param int    $port Port of the InfluxDB process
     */
    public function __construct($host, $port)
    {
        $this->config['host'] = $host;
        $this->config['port'] = $port;
    }

    /**
     * Close the stream (if created)
     */
    public function __destruct()
    {
        if (isset($this->stream) && is_resource($this->stream)) {
            fclose($this->stream);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setParameters(array $parameters)
    {
        $this->parameters = $parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function getParameters()
    {
        return $this->parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function write($data = null)
    {
        if (isset($this->stream) === false) {
            $this->createStream();
        }

        $this->doWrite($data);
    }

    /**
     * {@inheritdoc}
     */
    abstract public function isSuccess();

    /**
     * Perform write to socket
     * @param mixed|null $data
     */
    abstract protected function doWrite($data = null);

    /**
     * Create the resource stream
     */
    abstract protected function createStream();

}
