<?php
/**
 * @author Heinz Wiesinger
 */

namespace InfluxDB\Driver;

use \InfluxDB\Client;
use \InfluxDB\ResultSet;
use \Requests_Session;
use \Requests_Response;
use \Requests_Exception;

/**
 * Class Requests
 *
 * @package InfluxDB\Driver
 */
class Requests implements DriverInterface, QueryDriverInterface
{

    /**
     * Array of options
     *
     * @var array
     */
    private $parameters;

    /**
     * @var Requests_Session
     */
    private $httpClient;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var Requests_Response
     */
    private $response;

    /**
     * Set the config for this driver
     *
     * @param Requests_Session $httpClient
     * @param Client           $client
     *
     * @return mixed
     */
    public function __construct(Requests_Session $httpClient, Client $client)
    {
        $this->httpClient = $httpClient;
        $this->client = $client;
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
     *
     * @throws Exception
     * @return mixed
     */
    public function write($data = null)
    {
        return $this->request($data);
    }

    /**
     * @throws Exception
     * @return ResultSet
     */
    public function query()
    {
        $raw = $this->request();

        return new ResultSet($raw);
    }

    /**
     * @throws Exception
     * @return string
     */
    protected function request($data = null)
    {
        $this->response = null;

        $url = sprintf('%s/%s', $this->client->getBaseURI(), $this->parameters['url']);

        if (isset($this->parameters['auth'])) {
            $options['auth'] = $this->parameters['auth'];
        }

        $options['timeout'] = $this->client->getTimeout();
        $options['verify'] = $this->client->getVerifySSL();

        $this->response = $this->httpClient->request($url, [], $data, strtoupper($this->parameters['method']), $options);

        return (string) $this->response->body;
    }

    /**
     * Should return if sending the data was successful
     *
     * @return bool
     */
    public function isSuccess()
    {
        if ($this->response === null) {
            return false;
        }

        $this->response->throw_for_status();

        return $this->response->success;
    }

}

