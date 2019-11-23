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
     * @var stream
     */
    private $stream;

    /**
     * @param stream $stream
     */
    public function __construct($stream)
    {
        $this->stream = $stream;
    }

    /**
     * @throws \InvalidArgumentException
     * @throws ClientException
     * @return array $parsedResults
     */
    private function getParsedResults()
    {
        if(is_null($this->parsedResults)) {
            $this->parsedResults=json_decode(stream_get_contents($this->stream), true)['results'];
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new \InvalidArgumentException('Invalid JSON');
            }
            $this->validate($this->parsedResults);
        }

        return $this->parsedResults;
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
        $json_pointer="/results/$measurement/series";
        rewind($this->stream);
        foreach(JsonMachine::fromStream($this->stream, $json_pointer) as $series) {
            foreach($series['values'] as $point) {
                $points = array_combine($series['columns'], $point);
                if (!empty($series['tags'])) {
                    $points += $series['tags'];
                }
                yield $points;
            }
        }
        rewind($this->stream);
    }
}
