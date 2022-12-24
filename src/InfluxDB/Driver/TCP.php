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
class TCP extends AbstractSocketDriver implements DriverInterface
{

    private $result;

    /**
     * {@inheritdoc}
     */
    public function isSuccess()
    {
        return (bool) $this->result;
    }

    /**
     * {@inheritdoc}
     */
    protected function createStream()
    {
        $host = sprintf('tcp://%s:%d', $this->config['host'], $this->config['port']);

        // stream the data using TCP and suppress any errors
        $this->stream = @stream_socket_client($host);
    }

    /**
     * {@inheritdoc}
     */
    protected function doWrite($data)
    {
        $this->result = false !== @fwrite($this->stream, "$data\n");
    }
}
