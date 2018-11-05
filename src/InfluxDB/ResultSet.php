<?php

namespace InfluxDB;

use InfluxDB\Client\Exception as ClientException;

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
    protected $parsedResults = [];

    /**
     * @var string
     */
    protected $rawResults = '';

    /**
     * @param string $raw
     * @throws \InvalidArgumentException
     * @throws ClientException
     */
    public function __construct($raw)
    {
        $this->rawResults = $raw;
        $this->parsedResults = json_decode((string)$raw, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \InvalidArgumentException('Invalid JSON');
        }

        $this->validate();
    }

    /**
     * @throws ClientException
     */
    protected function validate()
    {
        // There was an error in the query thrown by influxdb
        if (isset($this->parsedResults['error'])) {
            throw new ClientException($this->parsedResults['error']);
        }

        // Check if there are errors in the first serie
        if (isset($this->parsedResults['results'][0]['error'])) {
            throw new ClientException($this->parsedResults['results'][0]['error']);
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
        $results = $this->parsedResults['results'];

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
}
