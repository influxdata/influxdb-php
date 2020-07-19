<?php

namespace InfluxDB;

use InfluxDB\Client\Exception as ClientException;
use JsonMachine\JsonMachine;
use JsonMachine\StreamBytes;

/**
 * Class ResultSet
 *
 * @package InfluxDB
 * @author  Stephen "TheCodeAssassin" Hoogendijk
 */
class ResultSet
{

    /**
     * @var array|mixed
     */
    private $parsedResults;

    /**
     * @var string
     */
    protected $rawResults = '';

    /**
     * @var array|mixed
     */
    private $parsedResultsMap;

    /**
     * @var stream
     */
    private $stream;

    /**
     * @var int
     */
    private $maxParseSize;

    /**
     * @var int
     */
    private $streamSize;

    const MAX_PARSE_SIZE = 1000000;

    /**
     * @param stream $stream
     */
    public function __construct($stream, ?int $maxParseSize=null)
    {
        $this->stream = $stream;
        $this->maxParseSize = $maxParseSize??self::MAX_PARSE_SIZE;
    }

    /**
     * @throws \InvalidArgumentException
     * @throws ClientException
     * @return array $parsedResults
     */
    private function getParsedResults()
    {
        if(is_null($this->parsedResults)) {
            $this->rawResults=stream_get_contents($this->stream);
            $this->parsedResults=json_decode($this->rawResults, true)['results'];
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new \InvalidArgumentException('Invalid JSON');
            }
            $this->validate($this->parsedResults);
        }
        return $this->parsedResults;
    }

    private function getParsedResultsMap()
    {
        if(is_null($this->parsedResultsMap)) {
            $this->parsedResultsMap=[];
            foreach($this->getParsedResults() as $index=>$values) {
                $this->parsedResultsMap[$values['time']]=$index;
            }
        }
        return $this->parsedResultsMap;
    }

    /**
     * @param array $parsedResults
     * @throws ClientException
     */
    private function validate($parsedResults)
    {
        // There was an error in the query thrown by influxdb
        if (isset($parsedResults['error'])) {
            throw new ClientException($parsedResults['error']);
        }

        // Check if there are errors in the first serie
        if (isset($parsedResults['results'][0]['error'])) {
            throw new ClientException($parsedResults['results'][0]['error']);
        }
    }

    /**
     * @return string
     */
    public function getRaw()
    {
        return $this->rawResults;
    }

    /**
     * @param  $metricName
     * @param  array $tags
     * @return array $points
     */
    public function getPoints($metricName = '', array $tags = [])
    {
        $points = [];
        $series = $this->getSeries();

        $emptyArgsProvided = empty($metricName) && empty($tags);
        foreach ($series as $serie) {
            if (($emptyArgsProvided
                || $serie['name'] === $metricName
                || (isset($serie['tags']) && array_intersect($tags, $serie['tags'])))
                && isset($serie['values'])
            ) {
                foreach ($this->getPointsFromSerie($serie) as $point) {
                    $points[] = $point;
                }
            }
        }

        return $points;
    }

    /**
     * @see: https://influxdb.com/docs/v0.9/concepts/reading_and_writing_data.html
     *
     * results is an array of objects, one for each query,
     * each containing the keys for a series
     *
     * @param int $queryIndex which Nth query result to return. Use null as value if you want all result of multi query results
     * @return array $series
     * @throws ClientException
     */
    public function getSeries($queryIndex = 0)
    {
        $results = $this->getParsedResults();

        if ($queryIndex !== null && !array_key_exists($queryIndex, $results)) {
            throw new \InvalidArgumentException('Invalid statement index provided');
        }

        $queryResults = [];
        foreach ($results as $index => $query) {
            /**
             * 'statement_id' was introduced in 1.2+ version so for backwards compatibility use array index for query index
             * See difference:
             *  1.2 -> https://docs.influxdata.com/influxdb/v1.2/guides/querying_data/
             *  1.1 -> https://docs.influxdata.com/influxdb/v1.1/guides/querying_data/
             */
            $statementId = isset($query['statement_id']) ? $query['statement_id'] : $index;

            if ($queryIndex === $statementId) {
                return $this->extractQuery($query);
            }

            if ($queryIndex === null) {
                $queryResults[$statementId] = $this->extractQuery($query);
            }
        }

        return $queryResults;
    }

    private function extractQuery($statementSeries)
    {
        if (isset($statementSeries['error'])) {
            throw new ClientException($statementSeries['error']);
        }

        return isset($statementSeries['series']) ? $statementSeries['series'] : [];
    }

    /**
     * @param int $queryIndex which Nth query result to return. Defaults to first query.
     * @return mixed
     * @throws ClientException
     */
    public function getColumns($queryIndex = 0)
    {
        if ($queryIndex === null) {
            $queryIndex = 0;
        }
        return $this->getSeries($queryIndex)[0]['columns'];
    }

    /**
     * @param  array $serie
     * @return array
     */
    private function getPointsFromSerie(array $serie)
    {
        $points = [];

        foreach ($serie['values'] as $value) {
            $point = array_combine($serie['columns'], $value);

            if (isset($serie['tags'])) {
                $point += $serie['tags'];
            }

            $points[] = $point;
        }

        return $points;
    }

    /**
     * Used to obtain large result sets.
     * @param  int $measurement
     * @throws JsonMachine\Exception\SyntaxError
     * @yield array
     */
    public function iterate(int $measurement=0)
    {
        if($this->getResponseSize() <= $this->maxParseSize) {
            foreach($this->getPoints() as $point) {
                yield $point;
            }
        }
        else {
            $json_pointer="/results/$measurement/series";
            foreach(JsonMachine::fromStream($this->stream, $json_pointer) as $series) {
                foreach($series['values'] as $point) {
                    $point = array_combine($series['columns'], $point);
                    if (!empty($series['tags'])) {
                        $point += $series['tags'];
                    }
                    yield $point;
                }
            }
            rewind($this->stream);
        }
    }

    public function getByTime(string $time, int $measurement=0):?array
    {
        if(\DateTime::createFromFormat(\DateTime::RFC3339, $time) === FALSE) {
            throw new \InvalidArgumentException("'$time' is not a valid RFC3339 time");
        }
        if($this->getResponseSize() <= $this->maxParseSize) {
            $point = $this->getParsedResultsMap()[$time]??null;
        }
        else {
            $json_pointer="/results/$measurement/series";
            $point=null;
            foreach(JsonMachine::fromStream($this->stream, $json_pointer) as $series) {
                foreach($series['values'] as $p) {
                    if($time===$p[0]) {
                        $p = array_combine($series['columns'], $p);
                        if (!empty($series['tags'])) {
                            $p += $series['tags'];
                        }
                        $point=$p;
                        break(2);
                    }
                }
            }
            rewind($this->stream);
        }
        return $point;
    }

    public function getResponseSize():int
    {
        //rewind($this->stream);
        if(!$this->streamSize) $this->streamSize = fstat($this->stream)['size'];
        return $this->streamSize;
    }
}
