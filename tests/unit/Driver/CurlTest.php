<?php

// mock namespace for unit tests to replace curl methods
namespace InfluxDB\Driver {

    function curl_init()
    {
        return null;
    }

    function curl_setopt($curl, $opt, $value)
    {
        \InfluxDB\Test\unit\Driver\CurlTest::$MOCK_OPTS[$opt] = $value;
    }

    function curl_exec()
    {
        return \InfluxDB\Test\unit\Driver\CurlTest::$MOCK_RESPONSE;
    }

    function curl_getinfo()
    {
        return \InfluxDB\Test\unit\Driver\CurlTest::$MOCK_INFO;
    }

    function curl_close()
    {
    }

    function curl_errno()
    {
        return 999;
    }
}


namespace InfluxDB\Test\unit\Driver {

    use InfluxDB\Driver\Curl;
    use InfluxDB\ResultSet;
    use PHPUnit\Framework\TestCase;
    use DMS\PHPUnitExtensions\ArraySubset\ArraySubsetAsserts;

    /**
     * @requires extension curl
     */
    class CurlTest extends TestCase
    {
        use ArraySubsetAsserts;

        static $MOCK_RESPONSE;
        static $MOCK_OPTS;
        static $MOCK_INFO;

        public function setUp(): void
        {
            parent::setUp();

            static::$MOCK_RESPONSE = false;
            static::$MOCK_INFO = [];
            static::$MOCK_OPTS = [];
        }

        protected function skipIsCurlTooOld()
        {
            $curlVersion = curl_version()['version'];
            if (version_compare($curlVersion, '7.40.0', '<')) {
                $this->markTestSkipped('Curl version is too low to support unix domain sockets. Version: ' . $curlVersion);
            }
        }

        /**
         * @requires PHP 7.0.7
         */
        public function testUnixDomainSocketDriverConstruction()
        {
            $this->skipIsCurlTooOld();
            $driver = new Curl('unix:///var/run/influxdb/influxdb.sock');

            $this->assertEquals('http://localhost', $driver->getDsn());
            $this->assertArraySubset([CURLOPT_UNIX_SOCKET_PATH => '/var/run/influxdb/influxdb.sock'], $driver->getCurlOptions());

        }

        public function testHttpDriverConstruction()
        {
            $driver = new Curl('http://localhost:8086');

            $this->assertEquals('http://localhost:8086', $driver->getDsn());
            $this->assertArrayNotHasKey(10231, $driver->getCurlOptions());
        }

        public function testParameters()
        {
            $driver = new Curl('http://localhost:8086');

            $parameters = ['auth' => ['user', 'password']];
            $driver->setParameters($parameters);

            $this->assertEquals($parameters, $driver->getParameters());
        }

        public function testGetCurlOptions()
        {
            $driver = new Curl('http://localhost:8086', [CURLOPT_USERAGENT => 'influxdb-php-lib']);

            $this->assertEquals(
                [
                    CURLOPT_USERAGENT => 'influxdb-php-lib',
                    CURLOPT_CONNECTTIMEOUT => 5,
                    CURLOPT_TIMEOUT => 10,
                ],
                $driver->getCurlOptions()
            );
        }

        public function testGetCurlOptionsAuth()
        {
            $driver = new Curl('http://localhost:8086', [CURLOPT_USERAGENT => 'influxdb-php-lib']);

            $parameters = ['auth' => ['user', 'password']];
            $driver->setParameters($parameters);

            $this->assertEquals(
                [
                    CURLOPT_USERAGENT => 'influxdb-php-lib',
                    CURLOPT_CONNECTTIMEOUT => 5,
                    CURLOPT_TIMEOUT => 10,
                    CURLOPT_USERPWD => 'user:password',
                ],
                $driver->getCurlOptions()
            );
        }

        public function testQuery()
        {
            $driver = new Curl('http://localhost:8086', [CURLOPT_USERAGENT => 'influxdb-php-lib']);

            static::$MOCK_INFO = ['http_code' => 200];
            static::$MOCK_RESPONSE = file_get_contents(__DIR__ . '/../json/result.example.json');
            $driver->setParameters(['url' => 'query?show measurements']);

            $result = $driver->query();

            $this->assertInstanceOf(ResultSet::class, $result);
            $this->assertEquals(static::$MOCK_RESPONSE, $result->getRaw());
            $this->assertEquals(static::$MOCK_INFO, $driver->getLastRequestInfo());

            $this->assertArraySubset(
                [
                    CURLOPT_URL => 'http://localhost:8086/query?show measurements',
                    CURLOPT_USERAGENT => 'influxdb-php-lib',
                ],
                static::$MOCK_OPTS
            );
        }

        public function testWrite()
        {
            $driver = new Curl('http://localhost:8086');

            static::$MOCK_INFO = ['http_code' => 200];
            static::$MOCK_RESPONSE = '';
            $driver->setParameters(['url' => 'write?something']);

            $driver->write(['data']);

            $this->assertArraySubset(
                [
                    CURLOPT_POST => 1,
                    CURLOPT_POSTFIELDS => ['data'],
                ],
                static::$MOCK_OPTS
            );
            $this->assertTrue($driver->isSuccess());

        }

        /**
         * @requires PHP 7.0.7
         */
        public function testWriteUnixDomainSocket()
        {
            $this->skipIsCurlTooOld();
            $driver = new Curl('unix:///var/run/influxdb/influxdb.sock', [CURLOPT_USERAGENT => 'influxdb-php-lib']);

            static::$MOCK_INFO = ['http_code' => 200];
            static::$MOCK_RESPONSE = '';
            $driver->setParameters(['url' => 'write?something']);

            $driver->write(['data']);

            $this->assertArraySubset(
                [
                    CURLOPT_UNIX_SOCKET_PATH => '/var/run/influxdb/influxdb.sock',
                    CURLOPT_POST => 1,
                    CURLOPT_POSTFIELDS => ['data'],
                    CURLOPT_USERAGENT => 'influxdb-php-lib',
                ],
                static::$MOCK_OPTS
            );
            $this->assertTrue($driver->isSuccess());

        }

        public function testIsSuccessThrowsExceptionOnHttpError()
        {
            $driver = new Curl('http://localhost:8086');

            static::$MOCK_INFO = ['http_code' => 500];
            static::$MOCK_RESPONSE = '';
            $driver->setParameters(['url' => 'write?something']);

            $driver->write(['data']);

            $this->expectException(\InfluxDB\Driver\Exception::class);
            $this->expectExceptionMessage('Request failed with HTTP Code 500');
            $driver->isSuccess();
        }

        public function testRequestThrowsExceptionWhenResultIsMissing()
        {
            $driver = new Curl('http://localhost:8086');

            static::$MOCK_RESPONSE = false;
            $driver->setParameters(['url' => 'write?something']);

            $this->expectException(\InfluxDB\Driver\Exception::class);
            $this->expectExceptionMessage('Request failed! curl_errno: 999');
            $driver->write(['data']);
        }

    }
}

