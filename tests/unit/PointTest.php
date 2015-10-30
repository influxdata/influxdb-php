<?php

namespace InfluxDB\Test;


use InfluxDB\Point;

class PointTest extends \PHPUnit_Framework_TestCase
{
    public function testPointStringRepresentation()
    {
        $expected = 'instance,host=server01,region=us-west cpucount=10i,free=1i,test="string",bool=false 1440494531376778481';

        $point =  new Point(
            'instance', // the name of the measurement
            null, // measurement value
            ['host' => 'server01', 'region' => 'us-west'],
            ['cpucount' => 10, 'free' => 1, 'test' => "string", 'bool' => false],
            '1440494531376778481'
        );


        $this->assertEquals($expected, (string) $point);
    }

    /**
     * Check if the Point class throw an exception when invalid timestamp are given.
     *
     * @dataProvider wrongTimestampProvider
     * @expectedException InfluxDB\Database\Exception
     */
    public function testPointWrongTimestamp($timestamp)
    {
        new Point(
           'instance', // the name of the measurement
            null, // measurement value
            ['host' => 'server01', 'region' => 'us-west'],
            ['cpucount' => 10, 'free' => 1, 'test' => "string", 'bool' => false],
            $timestamp
        );
    }

    /**
     * Check if the Point class accept all valid timestamp given.
     *
     * @dataProvider validTimestampProvider
     */
    public function testPointValidTimestamp($timestamp)
    {
        $expected = 'instance,host=server01,region=us-west cpucount=10i,free=1i,test="string",bool=false' . (($timestamp) ? ' ' . $timestamp : '');

        $point = new Point(
           'instance', // the name of the measurement
            null, // measurement value
            ['host' => 'server01', 'region' => 'us-west'],
            ['cpucount' => 10, 'free' => 1, 'test' => "string", 'bool' => false],
            $timestamp
        );

        $this->assertEquals($expected, (string) $point);
    }

    /**
     * Provide wrong timestamp value for testing.
     */
    public function wrongTimestampProvider()
    {
        return array(
            array('2015-10-27 14:17:40'),
            array('INVALID'),
            array('aa778481'),
            array('1477aee'),
            array('15.258'),
            array('15,258'),
            array(15.258),
            array(true)
        );
    }

    /**
     * Provide valie timestamp value for testing.
     */
    public function validTimestampProvider()
    {
        return array(
            array(time()),               // Current time returned by the PHP time function.
            array(0),                    // Day 0
            array(~PHP_INT_MAX),         // Minimum value integer
            array(PHP_INT_MAX),          // Maximum value integer
            array('1440494531376778481') // Text timestamp
        );
    }
}
