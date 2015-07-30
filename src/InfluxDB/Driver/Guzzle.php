<?php
/**
 * @author Stephen "TheCodeAssassin" Hoogendijk
 */

namespace InfluxDB\Driver;

use GuzzleHttp\Client;
use Guzzle\Http\Message\Response;
use GuzzleHttp\Psr7\Request;
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
     * Array of optional request options added to Guzzle request
     * 
     * @var array
     */
    private $requestOptions;
    
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
     * @return mixed
     */
    public function __construct(Client $client)
    {
        $this->httpClient = $client;
        $this->requestOptions = [];
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
     * Set optional request options like auth parameters as described in 
     * http://guzzle.readthedocs.org/en/latest/request-options.html
     * 
     * @param array $options
     */
    public function setRequestOptions(array $requestOptions)
    {   
        $this->requestOptions = $requestOptions;
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
        $requestOptions = array_merge(['body' => $data], $this->requestOptions);
        
        $this->response = $this->httpClient->post($this->parameters['url'], $requestOptions);
    }

    /**
     * @return ResultSet
     */
    public function query()
    {
        $response = $this->httpClient->get($this->parameters['url'], $this->requestOptions);

        $raw = (string) $response->getBody();

        return new ResultSet($raw);

    }

    /**
     * Should return if sending the data was successful
     *
     * @return bool
     */
    public function isSuccess()
    {
        return in_array($this->response->getStatusCode(), ['200', '204']);
    }
}
