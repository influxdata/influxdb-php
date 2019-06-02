<?php

namespace InfluxDB;

use Exception;
use InvalidArgumentException;
use InfluxDB\Exception as InfluxDBException;
use InfluxDB\Database\Exception as DatabaseException;
use InfluxDB\Database\RetentionPolicy;
use InfluxDB\Query\Builder as QueryBuilder;

/**
 * Class Database
 *
 * @package InfluxDB
 * @author  Stephen "TheCodeAssassin" Hoogendijk
 */
class Database
{
    const ENDPOINT_QUERY = "/query";
    const ENDPOINT_WRITE = "/write";

    /**
     * The name of the Database
     *
     * @var string
     */
    protected $name = '';

    /**
     * @var Client
     */
    protected $client;

    /**
     * Precision constants
     */
    const PRECISION_NANOSECONDS = 'ns';
    const PRECISION_MICROSECONDS_U = "µ";
    const PRECISION_MICROSECONDS = 'u';
    const PRECISION_MILLISECONDS = 'ms';
    const PRECISION_SECONDS = 's';
    const PRECISION_MINUTES = 'm';
    const PRECISION_HOURS = 'h';
    const PRECISION_RFC3339 = "rfc3339";

    /**
     * Construct a database object
     *
     * @param string $name
     * @param Client $client
     *
     * @throws \InvalidArgumentException
     */
    public function __construct($name, Client $client)
    {
        if (empty($name)) {
            throw new InvalidArgumentException('No database name provided');
        }

        $this->name = (string) $name;
        $this->client = $client;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * Query influxDB
     *
     * @param  string $query
     * @param  array  $params
     * @return ResultSet
     * @throws Exception
     */
    public function query($query, $params = [])
    {
        return $this->client->query($this->name, $query, $params);
    }

    /**
     * Create this database
     *
     * @param  RetentionPolicy $retentionPolicy
     * @param  bool            $createIfNotExists Deprecated parameter - to be removed
     * @return ResultSet
     * @throws DatabaseException
     */
    public function create(RetentionPolicy $retentionPolicy = null, $createIfNotExists = false)
    {
        if ($createIfNotExists) {
            trigger_error('The $createIfNotExists parameter to Database::create is deprecated', E_USER_DEPRECATED);
        }
        try {
            $query = sprintf('CREATE DATABASE "%s"', $this->name);

            $this->query($query);

            if ($retentionPolicy) {
                $this->createRetentionPolicy($retentionPolicy);
            }
        } catch (Exception $e) {
            throw new DatabaseException(
                sprintf('Failed to created database %s', $this->name),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param  RetentionPolicy $retentionPolicy
     * @return ResultSet
     */
    public function createRetentionPolicy(RetentionPolicy $retentionPolicy)
    {
        return $this->query($this->getRetentionPolicyQuery('CREATE', $retentionPolicy));
    }

    /**
     * Write points into InfluxDB using the current driver. This is the recommended method for inserting
     * data into InfluxDB.
     *
     * @param  Point[]     $points           Array of Point objects
     * @param  string      $precision        The timestamp precision (defaults to nanoseconds).
     * @param  string|null $retentionPolicy  Specifies an explicit retention policy to use when writing all points. If
     *                                       not set, the default retention period will be used. This is only
     *                                       applicable for the Guzzle driver. The UDP driver utilizes the endpoint
     *                                       configuration defined in the server's influxdb configuration file.
     * @return bool
     * @throws \InfluxDB\Exception
     */
    public function writePoints(array $points, $precision = self::PRECISION_NANOSECONDS, $retentionPolicy = null)
    {
        $payload = array_map(
            function (Point $point) {
                return (string) $point;
            },
            $points
        );

        return $this->writePayload($payload, $precision, $retentionPolicy);
    }

    /**
     * Write a payload into InfluxDB using the current driver. This method is similar to <tt>writePoints()</tt>,
     * except it takes a string payload instead of an array of Points. This is useful in the following situations:
     *
     *   1) Performing unique queries that may not conform to the current Point standard.
     *   2) Inserting very large set of points into a measurement where looping via array_map() actually
     *      hurts performance as the payload may be calculated in advance by caller.
     *
     * @param  string|array  $payload          InfluxDB payload (Or array of payloads) that conform to the Line syntax.
     * @param  string        $precision        The timestamp precision (defaults to nanoseconds).
     * @param  string|null   $retentionPolicy  Specifies an explicit retention policy to use when writing all points. If
     *                                         not set, the default retention period will be used. This is only
     *                                         applicable for the Guzzle driver. The UDP driver utilizes the endpoint
     *                                         configuration defined in the server's influxdb configuration file.
     * @return bool
     * @throws \InfluxDB\Exception
     */
    public function writePayload($payload, $precision = self::PRECISION_NANOSECONDS, $retentionPolicy = null)
    {
        $precision = self::toValidQueryPrecision($precision, self::ENDPOINT_WRITE);
        try {
            $query_params = ['db'=>$this->name, 'precision'=>$precision];
            $parameters = [
                'url' => self::ENDPOINT_WRITE."?",
                'database' => $this->name,
                'method' => 'post'
            ];
            if ($retentionPolicy !== null) {
                $query_params['rp'] = $retentionPolicy;
            }
            $parameters['url'] .= http_build_query($query_params);
            return $this->client->write($parameters, $payload);

        } catch (Exception $e) {
            throw new InfluxDBException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * @return bool
     */
    public function exists()
    {
        $databases = $this->client->listDatabases();

        return in_array($this->name, $databases);
    }

    /**
     * @param RetentionPolicy $retentionPolicy
     */
    public function alterRetentionPolicy(RetentionPolicy $retentionPolicy)
    {
        $this->query($this->getRetentionPolicyQuery('ALTER', $retentionPolicy));
    }

    /**
     * @return array
     * @throws Exception
     */
    public function listRetentionPolicies()
    {
        return $this->query(sprintf('SHOW RETENTION POLICIES ON "%s"', $this->name))->getPoints();
    }

    /**
     * Drop this database
     */
    public function drop()
    {
        $this->query(sprintf('DROP DATABASE "%s"', $this->name));
    }

    /**
     * Retrieve the query builder
     *
     * @return QueryBuilder
     */
    public function getQueryBuilder(array $parameters=[])
    {
        return new QueryBuilder($this, $parameters);
    }

    /**
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param  string          $method
     * @param  RetentionPolicy $retentionPolicy
     * @return string
     */
    protected function getRetentionPolicyQuery($method, RetentionPolicy $retentionPolicy)
    {
        $query = sprintf(
            '%s RETENTION POLICY "%s" ON "%s" DURATION %s REPLICATION %s',
            $method,
            $retentionPolicy->name,
            $this->name,
            $retentionPolicy->duration,
            $retentionPolicy->replication
        );

        if ($retentionPolicy->default) {
            $query .= ' DEFAULT';
        }

        return $query;
    }

    public static function validatePrecision($strPrecision) {
        return (in_array($strPrecision, [self::PRECISION_HOURS, self::PRECISION_MINUTES, self::PRECISION_SECONDS, self::PRECISION_MILLISECONDS, self::PRECISION_MICROSECONDS, self::PRECISION_MICROSECONDS_U, self::PRECISION_NANOSECONDS, self::PRECISION_RFC3339], TRUE) ? TRUE : FALSE);
    } // fin validatePrecision()

    public static function toValidQueryPrecision($strPrecision, $type=self::ENDPOINT_QUERY) {
        switch ($type) {
            case self::ENDPOINT_QUERY:
                switch ($strPrecision) {
                    case self::PRECISION_RFC3339: return NULL;
                    case self::PRECISION_MICROSECONDS_U: return self::PRECISION_MICROSECONDS;
                } // fin switch
                break;
            case self::ENDPOINT_WRITE:
                switch ($strPrecision) {
                    case self::PRECISION_RFC3339: return self::PRECISION_NANOSECONDS;
                    case self::PRECISION_MICROSECONDS_U: return self::PRECISION_MICROSECONDS;
                } // fin switch
                break;
        } // fin switch
        return $strPrecision;
    } // fin toValidQueryPrecision()
}
