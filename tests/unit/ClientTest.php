<?php

namespace InfluxDB\Test;

use InfluxDB\Client;
use InfluxDB\Driver\Guzzle;

class ClientTest extends AbstractTest
{

    /**
     *
     */
    public function setUp()
    {
        parent::setUp();
    }

    /** @var Client $client */
    protected $client = null;

    public function testGetters()
    {
        $client = $this->getClient();

        $this->assertEquals('http://localhost:8086', $client->getBaseURI());
        $this->assertInstanceOf('InfluxDB\Driver\Guzzle', $client->getDriver());
        $this->assertEquals('localhost', $client->getHost());
        $this->assertEquals('0', $client->getTimeout());
    }

    public function testBaseURl()
    {
        $client = $this->getClient();

        $this->assertEquals($client->getBaseURI(), 'http://localhost:8086');
    }

    public function testSelectDbShouldReturnDatabaseInstance()
    {
        $client = $this->getClient();

        $dbName = 'test-database';
        $database = $client->selectDB($dbName);

        $this->assertInstanceOf('\InfluxDB\Database', $database);

        $this->assertEquals($dbName, $database->getName());
    }


    /**
     */
    public function testGuzzleQuery()
    {
        $client = $this->getClient();
        $query = "some-bad-query";

        $bodyResponse = file_get_contents(dirname(__FILE__) . '/result.example.json');
        $httpMockClient = $this->buildHttpMockClient($bodyResponse);

        $client->setDriver(new Guzzle($httpMockClient));

        /** @var \InfluxDB\ResultSet $result */
        $result = $client->query(null, $query);

        $this->assertInstanceOf('\InfluxDB\ResultSet', $result);
    }

    public function testGetLastQuery()
    {
        $this->mockClient->query('test', 'SELECT * from test_metric');
        $this->assertEquals($this->getClient()->getLastQuery(), 'SELECT * from test_metric');
    }

    protected function getClient()
    {
        return new Client('localhost', 8086);
    }

}