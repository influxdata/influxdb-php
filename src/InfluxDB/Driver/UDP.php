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
class UDP extends AbstractSocketDriver implements DriverInterface
{
    /**
     * {@inheritdoc}
     */
    public function isSuccess()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    protected function createStream()
    {
        $host = sprintf('udp://%s:%d', $this->config['host'], $this->config['port']);

        // stream the data using UDP and suppress any errors
        $this->stream = @stream_socket_client($host);
    }

    /**
     * {@inheritdoc}
     */
    protected function doWrite($data)
    {
        @stream_socket_sendto($this->stream, $data);
    }
}
