<?php
/**
 * Created by PhpStorm.
 * User: dmartinez
 * Date: 18-6-15
 * Time: 17:39
 */

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
}