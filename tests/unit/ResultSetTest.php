<?php

namespace InfluxDB\Test\unit;

use InfluxDB\ResultSet;
use PHPUnit\Framework\TestCase;

class ResultSetTest extends TestCase
{
    /** @var ResultSet $resultSet */
    protected $resultSet;

    /** @var ResultSet $resultSet */
    protected $multiQueryResultSet;

    public function setUp(): void
    {
        $resultJsonExample = file_get_contents(__DIR__ . '/json/result.example.json');
        $this->resultSet = new ResultSet($resultJsonExample);
        $this->multiQueryResultSet = new ResultSet(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'));
    }

    public function testThrowsExceptionIfJSONisNotValid()
    {
        $invalidJSON = 'foo';

        $this->expectException(\InvalidArgumentException::class);
        new ResultSet($invalidJSON);
    }

    /**
     * Throws Exception if something went wrong with influxDB
     */
    public function testThrowsInfluxDBException()
    {

        $errorResult = <<<EOD
{
    "series": [],
    "error": "Big error, many problems."
}
EOD;
        $this->expectException(\InfluxDB\Exception::class);
        new ResultSet($errorResult);
    }

    /**
     * Throws Exception if something went wrong with influxDB after processing the query
     */
    public function testThrowsInfluxDBExceptionIfAnyErrorInSeries()
    {
        $this->expectException(\InfluxDB\Exception::class);
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

    public function testGetSeriesFromMultiQuery()
    {
        $this->assertEquals(['time', 'value'], $this->multiQueryResultSet->getColumns(0));
        $this->assertEquals(['time', 'value'], $this->multiQueryResultSet->getColumns(null));
        $this->assertEquals(['time', 'count'], $this->multiQueryResultSet->getColumns(1));
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
        $series = $this->multiQueryResultSet->getSeries();

        $this->assertTrue(is_array($series));
        $this->assertCount(1, $series);
        $this->assertEquals('cpu_load_short', $series[0]['name']);
    }

    public function testGetNthResultFromMultiStatementQuery()
    {
        $series = $this->multiQueryResultSet->getSeries(1);

        $this->assertTrue(is_array($series));
        $this->assertCount(1, $series);
        $this->assertEquals('cpu_load_long', $series[0]['name']);
    }

    public function testGetAllResultsFromMultiStatementQuery()
    {
        $series = $this->multiQueryResultSet->getSeries(null);

        $this->assertTrue(is_array($series));
        $this->assertCount(2, $series);
        $this->assertEquals('cpu_load_short', $series[0][0]['name']);
        $this->assertEquals('cpu_load_long', $series[1][0]['name']);
    }

    /**
     * Throws Exception if invalid query index is given
     */
    public function testGetInvalidResultFromMultiStatementQuery()
    {

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid statement index provided');
        $this->multiQueryResultSet->getSeries(2);
    }

    /**
     * Throws Exception if Nth query resulted an error
     */
    public function testGetErrorFromMultiStatementQuery()
    {
        $raw = json_decode(file_get_contents(__DIR__ . '/json/result-multi-query.example.json'), true);
        unset($raw['results'][1]['series']);
        $raw['results'][1]['error'] = 'should trigger error';

        $resultSet = new ResultSet(json_encode($raw));

        $this->expectException(\InfluxDB\Client\Exception::class);
        $this->expectExceptionMessage('should trigger error');
        $resultSet->getSeries(1);
    }
}