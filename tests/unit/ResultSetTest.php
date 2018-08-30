<?php
namespace InfluxDB\Test\unit;

use InfluxDB\ResultSet;
use PHPUnit\Framework\TestCase;

class ResultSetTest extends TestCase
{
    /** @var ResultSet  $resultSet*/
    protected $resultSet;

    public function setUp()
    {
        $resultJsonExample = file_get_contents(__DIR__ . '/json/result.example.json');
        $this->resultSet = new ResultSet($resultJsonExample);
    }

    /**
     * @expectedException \InvalidArgumentException
     */
    public function testThrowsExceptionIfJSONisNotValid()
    {
        $invalidJSON = 'foo';

        new ResultSet($invalidJSON);
    }

    /**
     * Throws Exception if something went wrong with influxDB
     * @expectedException \InfluxDB\Exception
     */
    public function testThrowsInfluxDBException()
    {

        $errorResult = <<<EOD
{
    "series": [],
    "error": "Big error, many problems."
}
EOD;
        new ResultSet($errorResult);
    }

    /**
     * Throws Exception if something went wrong with influxDB after processing the query
     * @expectedException \InfluxDB\Exception
     */
    public function testThrowsInfluxDBExceptionIfAnyErrorInSeries()
    {
        new ResultSet(file_get_contents(__DIR__ . '/json/result-error.example.json'));
    }

    public function testGetRaw()
    {
        $resultJsonExample = file_get_contents(__DIR__ . '/json/result.example.json');
        $resultSet = new ResultSet($resultJsonExample);

        $this->assertEquals($resultJsonExample, $resultSet->getRaw());
    }

    /**
     * We can get points from measurement
     */
    public function testGetPointsFromNameWithoudTags()
    {
        $resultJsonExample = file_get_contents(__DIR__ . '/json/result-no-tags.example.json');
        $this->resultSet = new ResultSet($resultJsonExample);

        $measurementName = 'cpu_load_short';
        $expectedNumberOfPoints = 2;

        $points = $this->resultSet->getPoints($measurementName);

        $this->assertTrue(is_array($points));

        $this->assertCount($expectedNumberOfPoints, $points);
    }

    /**
     * We can get points from measurement
     */
    public function testGetPoints()
    {
        $expectedNumberOfPoints = 3;

        $points = $this->resultSet->getPoints();

        $this->assertTrue(
            is_array($points)
        );

        $this->assertCount($expectedNumberOfPoints, $points);

    }

    public function testGetSeries()
    {
        $this->assertEquals(['time', 'value'], $this->resultSet->getColumns());
    }

    /**
     * We can get points from measurement
     */
    public function testGetPointsFromMeasurementName()
    {
        $measurementName = 'cpu_load_short';
        $expectedNumberOfPoints = 2;
        $expectedValueFromFirstPoint = 0.64;

        $points = $this->resultSet->getPoints($measurementName);

        $this->assertTrue(
            is_array($points)
        );

        $this->assertCount($expectedNumberOfPoints, $points);

        $somePoint = array_shift($points);

        $this->assertEquals($expectedValueFromFirstPoint, $somePoint['value']);
    }

    public function testGetPointsFromTags()
    {
        $tags = ['host' => 'server01'];
        $expectedNumberOfPoints = 2;

        $points = $this->resultSet->getPoints('', $tags);

        $this->assertTrue(is_array($points));
        $this->assertCount($expectedNumberOfPoints, $points);
    }

    public function testGetPointsFromNameAndTags()
    {
        $tags = ['host' => 'server01'];
        $expectedNumberOfPoints = 2;

        $points = $this->resultSet->getPoints('', $tags);

        $this->assertTrue(is_array($points));
        $this->assertCount($expectedNumberOfPoints, $points);
    }

    public function testGetDefaultResultFromMultiStatementQuery()
    {
        $resultSet = new ResultSet(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'));

        $series = $resultSet->getSeries();

        $this->assertTrue(is_array($series));
        $this->assertCount(1, $series);
        $this->assertEquals('cpu_load_short', $series[0]['name']);
    }

    public function testGetNthResultFromMultiStatementQuery()
    {
        $resultSet = new ResultSet(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'));

        $series = $resultSet->getSeries(1);

        $this->assertTrue(is_array($series));
        $this->assertCount(1, $series);
        $this->assertEquals('cpu_load_long', $series[0]['name']);
    }

    public function testGetAllResultsFromMultiStatementQuery()
    {
        $resultSet = new ResultSet(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'));

        $series = $resultSet->getSeries(null);

        $this->assertTrue(is_array($series));
        $this->assertCount(2, $series);
        $this->assertEquals('cpu_load_short', $series[0][0]['name']);
        $this->assertEquals('cpu_load_long', $series[1][0]['name']);
    }

    /**
     * Throws Exception if invalid query index is given
     *
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage Invalid statement index provided
     */
    public function testGetInvalidResultFromMultiStatementQuery()
    {
        $resultSet = new ResultSet(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'));

        $resultSet->getSeries(2);
    }

    /**
     * Throws Exception if Nth query resulted an error
     *
     * @expectedException \InfluxDB\Client\Exception
     * @expectedExceptionMessage should trigger error
     */
    public function testGetErrorFromMultiStatementQuery()
    {
        $raw = json_decode(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'), true);
        unset($raw['results'][1]['series']);
        $raw['results'][1]['error'] = 'should trigger error';

        $resultSet = new ResultSet(json_encode($raw));

        $resultSet->getSeries(1);
    }
}