<?php

namespace InfluxDB\Driver;


use InfluxDB\ResultSet;

class Curl implements DriverInterface, QueryDriverInterface
{
    /**
     * request parameters
     *
     * @var array
     */
    private $parameters;

    /**
     * @var string
     */
    private $dsn;

    /**
     * Array of curl options
     *
     * @var array
     */
    private $options;

    /** @var array */
    protected $lastRequestInfo;

    /**
     * Build the Curl driver from a dsn
     * Examples:
     *
     * http://localhost:8086
     * https://localhost:8086
     * unix:///var/run/influxdb/influxdb.sock
     *
     * @param string $dsn
     * @param array $options options for curl requests. See http://php.net/manual/en/function.curl-setopt.php for available options
     * @throws Exception
     */
    public function __construct($dsn, $options = [])
    {
        if (!extension_loaded('curl')) {
            throw new Exception('Curl extension is not enabled!');
        }

        $this->dsn = $dsn;
        if (strpos($dsn, 'unix://') === 0) {
            if (PHP_VERSION_ID < 70007) {
                throw new Exception('Unix domain sockets are supported since PHP version PHP 7.0.7. Current version: ' . PHP_VERSION);
            }
            $curlVersion = curl_version()['version'];
            if (version_compare($curlVersion, '7.40.0', '<')) {
                throw new Exception('Unix domain sockets are supported since curl version 7.40.0! Current curl version: ' . $curlVersion);
            }
            $options[CURLOPT_UNIX_SOCKET_PATH] = substr($dsn, 7);

            $this->dsn = 'http://localhost';
        }

        $this->options = $options;
    }

    /**
     * Called by the client write() method, will pass an array of required parameters such as db name
     *
     * will contain the following parameters:
     *
     * [
     *  'database' => 'name of the database',
     *  'url' => 'URL to the resource',
     *  'method' => 'HTTP method used'
     * ]
     *
     * @param array $parameters
     *
     * @return mixed
     */
    public function setParameters(array $parameters)
    {
        $this->parameters = $parameters;
    }

    /**
     * @return array
     */
    public function getParameters()
    {
        return $this->parameters;
    }

    /**
     * Send the data
     *
     * @param $data
     * @throws Exception
     *
     * @return mixed
     */
    public function write($data = null)
    {
        $options = $this->getCurlOptions();
        $options[CURLOPT_POST] = 1;
        $options[CURLOPT_POSTFIELDS] = $data;


        $this->execute($this->parameters['url'], $options);
    }

    /**
     * Should return if sending the data was successful
     *
     * @return bool
     * @throws Exception
     */
    public function isSuccess()
    {
        if (empty($this->lastRequestInfo)) {
            return false;
        }
        $statusCode = $this->lastRequestInfo['http_code'];

        if (!in_array($statusCode, [200, 204], true)) {
            throw new Exception('Request failed with HTTP Code ' . $statusCode);
        }

        return true;
    }

    /**
     * @return ResultSet
     * @throws \InfluxDB\Client\Exception
     */
    public function query()
    {
        $response = $this->execute($this->parameters['url'], $this->getCurlOptions());
        return new ResultSet($response);
    }

    protected function execute($url, $curlOptions = [])
    {
        $this->lastRequestInfo = null;
        $ch = curl_init();

        foreach ($curlOptions as $option => $value) {
            curl_setopt($ch, $option, $value);
        }

        curl_setopt($ch, CURLOPT_URL, $this->dsn . '/' . $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);

        $result = curl_exec($ch);
        $this->lastRequestInfo = curl_getinfo($ch);

        if ($result === false) {
            // in case of total failure - socket/port is closed etc
            throw new Exception('Request failed! curl_errno: ' . curl_errno($ch));
        }


        curl_close($ch);

        return $result;
    }

    /**
     * Returns curl options
     *
     * @return array
     */
    public function getCurlOptions()
    {
        $opts = $this->options + [
                CURLOPT_CONNECTTIMEOUT => 5, // 5 seconds
                CURLOPT_TIMEOUT => 10, // 30 seconds
            ];

        if (isset($this->parameters['auth'])) {
            list($username, $password) = $this->parameters['auth'];
            $opts[CURLOPT_USERPWD] = $username . ':' . $password;
        }

        return $opts;
    }

    /**
     * Last curl request info
     *
     * @return array
     */
    public function getLastRequestInfo()
    {
        return $this->lastRequestInfo;
    }

    /**
     * @return string
     */
    public function getDsn()
    {
        return $this->dsn;
    }
}