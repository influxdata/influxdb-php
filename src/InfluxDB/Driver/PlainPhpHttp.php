<?php
/**
 * @author public@akademic.name
 */

namespace InfluxDB\Driver;

use InfluxDB\ResultSet;

/**
 * Class PlainPhpHttp
 *
 * @package InfluxDB\Driver
 */

class PlainPhpHttp implements DriverInterface, QueryDriverInterface {

    /**
     * Array of options
     *
     * @var array
     */
    private $parameters;

    /**
     * @var string
     */
    private $baseUri;

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
    public function __construct($baseUri) {
        $this->baseUri = $baseUri;
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
    public function setParameters(array $parameters) {
        $this->parameters = $parameters;
    }

    /**
     * @return array
     */
    public function getParameters() {
        return $this->parameters;
    }

    /**
     * Send the data
     *
     * @param $data
     *
     * @throws \Exception
     */
    public function write($data = null) {
        $full_url = $this->baseUri . '/' . $this->parameters['url'];
        $body = $this->getRequestParameters($data)['body'];

        $opts = [
            'http' => [
                'method' => 'POST',
                'content' => $body
            ]
        ];

        $context = stream_context_create($opts);

        $result = file_get_contents($full_url, false, $context);
        $this->response = new PlainPhpHttpResponse($http_response_header, $result);
    }

    /**
     * @throws \Exception
     * @return ResultSet
     */
    public function query() {
        $full_url = $this->baseUri . '/' . $this->parameters['url'];
        $result = file_get_contents($full_url);

        return new ResultSet($result);
    }

    /**
     * Should return if sending the data was successful
     *
     * @return bool
     * @throws Exception
     */
    public function isSuccess() {
        $statuscode = $this->response->getStatusCode();

        if( !in_array($statuscode, [200, 204], true) ) {
            throw new Exception('HTTP Code ' . $statuscode . ' ' . $this->response->getBody());
        }

        return true;
    }

    /**
     * @param null $data
     *
     * @return array
     */
    protected function getRequestParameters($data = null) {
        $requestParameters = ['http_errors' => false];

        if( $data ) {
            $requestParameters += ['body' => $data];
        }

        if( isset($this->parameters['auth']) ) {
            $requestParameters += ['auth' => $this->parameters['auth']];
        }

        return $requestParameters;
    }
}
