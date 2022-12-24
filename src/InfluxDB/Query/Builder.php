<?php

namespace InfluxDB\Query;

use InfluxDB\Database;
use InfluxDB\ResultSet;

/**
 * Class Builder
 *
 * Abstraction class for getting time series out of InfluxDB
 *
 * Sample usage:
 *
 * $series = new QueryBuilder($db);
 * $series->percentile(95)->setTimeRange($timeFrom, $timeTo)->getResult();
 *
 * $series->select('*')->from('*')->getResult();
 *
 * @todo add inner join
 * @todo add merge
 *
 * @package InfluxDB\Query
 * @author Stephen "TheCodeAssassin" Hoogendijk <s.hoogendijk@tech.leaseweb.com>
 */
class Builder
{
    /**
     * @var Database
     */
    protected $db;

    /**
     * @var string
     */
    protected $selection = '*';

    /**
     * @var string[]
     */
    protected $where = array();

    /**
     * @var string
     */
    protected $startTime;

    /**
     * @var string
     */
    protected $endTime;

    /**
     * @var string
     */
    protected $retentionPolicy;

    /**
     * @var string
     */
    protected $metric;

    /**
     * @var string
     */
    protected $limitClause = '';

    /**
     * @var string
     */
    protected $offsetClause = '';

    /**
     * @var array
     */
    protected $groupBy;

    /**
     * @var array
     */
    protected $orderBy;

    /**
     * @var string
     */
    protected $tz;

    /**
     * @param Database $db
     */
    public function __construct(Database $db)
    {
        $this->db = $db;
    }

    /**
     * @param  string $metric The metric to select (required)
     * @return $this
     */
    public function from($metric)
    {
        $this->metric = $metric;

        return $this;
    }

    /**
     * Custom select method
     *
     * example:
     *
     * $series->select('sum(value)',
     *
     * @param  string $customSelect
     * @return $this
     */
    public function select($customSelect)
    {
        $this->selection = $customSelect;

        return $this;
    }

    /**
     * @param array $conditions
     *
     * Example: array('time > now()', 'time < now() -1d');
     *
     * @return $this
     */
    public function where(array $conditions)
    {
        foreach ($conditions as $condition) {
            $this->where[] = $condition;
        }

        return $this;
    }

    /**
     * @param  string $field
     * @return $this
     */
    public function count($field = 'type')
    {
        $this->selection = sprintf('count(%s)', $field);

        return $this;
    }

    /**
     * @param  string $field
     * @return $this
     */
    public function median($field = 'type')
    {
        $this->selection = sprintf('median(%s)', $field);

        return $this;
    }

    /**
     * @param  string $field
     * @return $this
     */
    public function mean($field = 'type')
    {
        $this->selection = sprintf('mean(%s)', $field);

        return $this;
    }

    /**
     * @param  string $field
     * @return $this
     */
    public function sum($field = 'type')
    {
        $this->selection = sprintf('sum(%s)', $field);

        return $this;
    }

    /**
     * @param  string $field
     * @return $this
     */
    public function first($field = 'type')
    {
        $this->selection = sprintf('first(%s)', $field);

        return $this;
    }

    /**
     * @param  string $field
     * @return $this
     */
    public function last($field = 'type')
    {
        $this->selection = sprintf('last(%s)', $field);

        return $this;
    }

    public function groupBy($field = 'type') {
        $this->groupBy[] = $field;

        return $this;
    }

    public function orderBy($field = 'type', $order = 'ASC') {
        $this->orderBy[] = "$field $order";

        return $this;
    }

    /**
     * Set's the time range to select data from
     *
     * @param  int $from
     * @param  int $to
     * @return $this
     */
    public function setTimeRange($from, $to)
    {
        $fromDate = date('Y-m-d H:i:s', (int) $from);
        $toDate = date('Y-m-d H:i:s', (int) $to);

        $this->where(array("time > '$fromDate'", "time < '$toDate'"));

        return $this;
    }

    /**
     * @param int $percentile Percentage to select (for example 95 for 95th percentile billing)
     *
     * @return $this
     */
    public function percentile($percentile = 95)
    {
        $this->selection = sprintf('percentile(value, %d)', (int) $percentile);

        return $this;
    }

    /**
     * Limit the ResultSet to n records
     *
     * @param int $count
     *
     * @return $this
     */
    public function limit($count)
    {
        $this->limitClause = sprintf(' LIMIT %s', (int) $count);

        return $this;
    }

    /**
     * Offset the ResultSet to n records
     *
     * @param int $count
     *
     * @return $this
     */
    public function offset($count)
    {
        $this->offsetClause = sprintf(' OFFSET %s', (int) $count);

        return $this;
    }

    /**
     * Set the timezone
     *
     * @param string $timezone
     *
     * @return $this
     */
    public function tz($timezone)
    {
        $this->tz = $timezone;

        return $this;
    }

    /**
     * Add retention policy to query
     *
     * @param string $rp
     *
     * @return $this
     */
    public function retentionPolicy($rp)
    {
        $this->retentionPolicy =  $rp;

        return $this;
    }

    /**
     * @return string
     */
    public function getQuery()
    {
        return $this->parseQuery();
    }

    /**
     * Gets the result from the database (builds the query)
     *
     * @return ResultSet
     * @throws \Exception
     */
    public function getResultSet($params=[])
    {
        return  $this->db->query($this->parseQuery(), $params);
    }

    /**
     * @return string
     */
    protected function parseQuery()
    {
        $rp = '';

        if (is_string($this->retentionPolicy) && !empty($this->retentionPolicy)) {
            $rp = sprintf('"%s".', $this->retentionPolicy);
        }

        $query = sprintf('SELECT %s FROM %s"%s"', $this->selection, $rp, $this->metric);

        if (! $this->metric) {
            throw new \InvalidArgumentException('No metric provided to from()');
        }

        for ($i = 0, $iMax = count($this->where); $i < $iMax; $i++) {
            $selection = 'WHERE';

            if ($i > 0) {
                $selection = 'AND';
            }

            $clause = $this->where[$i];
            $query .= ' ' . $selection . ' ' . $clause;

        }

        if (!empty($this->groupBy)) {
            $query .= ' GROUP BY ' . implode(',', $this->groupBy);
        }

        if (!empty($this->orderBy)) {
            $query .= ' ORDER BY ' . implode(',', $this->orderBy);
        }

        if ($this->limitClause) {
            $query .= $this->limitClause;
        }

        if ($this->offsetClause) {
            $query .= $this->offsetClause;
        }

        if (isset($this->tz)) {
            //also add support for timezone offset
            $tz = is_numeric(trim($this->tz,0))?
                timezone_name_from_abbr('',$this->tz*3600,0):
                $this->tz;
            $query .= " tz('".$tz."')";
        }

        return $query;
    }
}
