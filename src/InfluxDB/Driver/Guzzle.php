<?php
/**
 * @author Stephen "TheCodeAssassin" Hoogendijk
 */

namespace InfluxDB\Driver;

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\StreamWrapper;
use InfluxDB\ResultSet;

/**
 * Class Guzzle
 *
 * @package InfluxDB\Driver
 */
class Guzzle implements DriverInterface, QueryDriverInterface
{

    /**
     * Array of options
     *
     * @var array
     */
    private $parameters;

    /**
     * @var Client
     */
    private $httpClient;

    /**
     * @var Response
     */
    private $response;

    /**
     * Set the config for this driver
     *
     * @param Client $client
     *
     */
    public function __construct(Client $client)
    {
        $this->httpClient = $client;
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
     * @throws \Exception
     */
    public function write($data = null)
    {
        $this->response = $this->httpClient->post($this->parameters['url'], $this->getRequestParameters($data));
    }

    /**
     * @throws \Exception
     * @return stream
     */
    public function query()
    {
        $response = $this->httpClient->get($this->parameters['url'], $this->getRequestParameters());

        return new ResultSet(StreamWrapper::getResource($response->getBody()));
    }

    /**
     * Should return if sending the data was successful
     *
     * @return bool
     * @throws Exception
     */
    public function isSuccess()
    {
        $statuscode = $this->response->getStatusCode();

        if(!in_array($statuscode, [200, 204], true))
        {
            throw new Exception('HTTP Code ' . $statuscode . ' ' . $this->response->getBody());
        }

        return true;
    }

    /**
     * @param null $data
     *
     * @return array
     */
    protected function getRequestParameters($data = null)
    {
        $requestParameters = ['http_errors' => false];

        if ($data) {
            $requestParameters += ['body' => $data];
        }

        if (isset($this->parameters['auth'])) {
            $requestParameters += ['auth' => $this->parameters['auth']];
        }

        return $requestParameters;
    }
}
