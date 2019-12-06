<?php

namespace InfluxDB\Query;

use InfluxDB\Database;
use InfluxDB\ResultSet;

class QueryBuilder
{
    const VALID_OPERATORS = ['=','<>','!=','>','>=','<','<='];
    const VALID_DURATIONS = ['ns'=>1/1000000000,'u'=>1/1000000,'ms'=>1/1000,'s'=>1,'m'=>60,'h'=>60*60,'d'=>60*60*24,'w'=>60*60*24*7];
    const VALID_PROPERTIES = ['select'=>[],'where'=>[],'measurement'=>'','limitClause'=>'','offsetClause'=>'','groupBy'=>[],'groupByTime'=>'','orderBy'=>[],'retentionPolicy'=>''];

    /**
    * @var Database
    */
    private $db;

    /**
    * @var string[]
    */
    private $select = [];

    /**
    * @var string[]
    */
    private $where = [];

    /**
    * @var string
    */
    private $retentionPolicy;

    /**
    * @var string
    */
    private $measurement;

    /**
    * @var string
    */
    private $limitClause = '';

    /**
    * @var string
    */
    private $offsetClause = '';

    /**
    * @var array
    */
    private $groupBy = [];

    private $groupByTime = '';

    private $fieldKeys;
    private $tagKeys;

    /**
    * @var array
    */
    private $orderBy = [];

    /**
    * @param Database $db
    */
    public function __construct(Database $db)
    {
        $this->db = $db;
    }

    /**
    * @param array $fields
    * @param array $methods
    *
    * Example: ['field1', 'field2'], ['min', 'max'];
    *
    * @return $this
    */
    public function select(array $fields, array $methods=[]):self
    {
        foreach($fields as $field) {
            if($methods) {
                foreach($methods as $method) {
                    $this->addFunctionField($field, $method);
                }
            }
            else {
                $this->select[$field] = $field;
            }
        }
        return $this;
    }

    public function reset(array $properties=[], bool $perserveRetention=true):self
    {
        if($properties) {
            if($err = array_diff_key($properties, array_keys(self::VALID_PROPERTIES))) {
                throw new \InvalidArgumentException(implode(', ', $err). ' are not valid properties');
            }
        }
        else {
            $properties=self::VALID_PROPERTIES;
            if(!$perserveRetention) {
                unset($properties['retentionPolicy']);
            }
            $properties=array_keys($properties);
        }
        foreach($properties as $property) {
            $this->$property=self::VALID_PROPERTIES[$property];
        }
        if(in_array('measurement', $properties)) {
            $this->fieldKeys=null;
            $this->tagKeys=null;
        }
        return $this;
    }

    /**
    * @param  string $measurement The measurement to select (required)
    * @return $this
    */
    public function from($measurement, bool $perserveRetention=true):self
    {
        $this->reset([], $perserveRetention);
        $this->measurement = $measurement;
        return $this;
    }

    /**
    * @param array $field
    *
    * Example: ['time > now()', 'time < now() -1d'];
    *
    * @return $this
    */
    public function where(string $field, string $operator, $value):self
    {
        if(!in_array($operator, self::VALID_OPERATORS)) throw new \InvalidArgumentException("Invalid operator: $operator");
        switch($field) {
            case 'time':
                if($this->validateRFCDate($value)) {
                    $this->where[] = "time $operator '$value'";
                }
                elseif($this->isLiteralTime($value)) {
                    $this->where[] = "time $operator $value";
                }
                elseif($this->isDigit($value)) {
                    $this->where[] = "time $operator {$value}s";
                }
                else throw new \InvalidArgumentException("'$value' is not a valided time");
                break;
            case 'timeOffset':
                if($value===0) $value=(string) $value;
                if(!$this->isLiteralTime($value)) throw new \InvalidArgumentException("'$value' is not a valided literal time");
                $this->where[] = "time $operator now()".($value?" -$value":'');
                break;
            default:
                $this->where[] = is_numeric($value)
                ?"$field $operator $value"
                :"$field $operator '$value'";
        }
        return $this;
    }

    private function addFunctionField(string $field, string $method):self
    {
        $method=strtoupper($method);
        switch($method) {
            case 'COUNT':
            case 'MEDIAN':
            case 'MEAN':
            case 'SUM':
            case 'FIRST':
            case 'MIN':
            case 'MAX':
            case 'SPREAD':
            case 'LAST':
                $this->select[strtolower($method).($field==='*'?'':'_'.$field)] = "$method($field)";
                break;
            default: throw new \InvalidArgumentException("Invalid method: $method");
        }
        return $this;
    }

    public function groupBy(string $field):self
    {
        $this->groupBy[] = $field;

        return $this;
    }

    public function groupByTime(string $field):self
    {
        $this->groupByTime = $field;

        return $this;
    }

    public function orderBy($field, $order = 'ASC'):self
    {
        $this->orderBy[] = "$field $order";

        return $this;
    }

    /**
    * @param int $percentile Percentage to select (for example 95 for 95th percentile billing)
    *
    * @return $this
    */
    public function percentile($percentile = 95):self
    {
        $this->select = sprintf('percentile(value, %d)', (int) $percentile);

        return $this;
    }

    /**
    * Limit the ResultSet to n records
    *
    * @param int $count
    *
    * @return $this
    */
    public function limit($count):self
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
    public function offset($count):self
    {
        $this->offsetClause = sprintf(' OFFSET %s', (int) $count);

        return $this;
    }

    public function subQuery($foo):self
    {
        $this->measurement = $foo;
        throw new \Exception('QueryBuilder::subQuery() is not complete');
        return $this;
    }

    /**
    * Add retention policy to query
    *
    * @param string $rp
    *
    * @return $this
    */
    public function retentionPolicy(string $rp):self
    {
        $this->retentionPolicy =  $rp;

        return $this;
    }

    /**
    * Gets the result from the database (builds the query)
    *
    * @return ResultSet
    * @throws \Exception
    */
    public function getResultSet():ResultSet
    {
        $this->validate();

        return  $this->db->query($this->getQuery());
    }

    /**
    * @return string
    */
    public function getQuery():string
    {
        //Future.  Add subqueries

        foreach($this->select as $alias=>$function) {
            $fields[]=$alias === $function
            ?$function
            :"$function AS $alias";
        }

        $query=['SELECT', implode(', ', $fields)];
        $query[]=$this->retentionPolicy
        ?"FROM $this->retentionPolicy.$this->measurement"
        :"FROM $this->measurement";
        if($this->where) $query[]='WHERE '.implode(' AND ', $this->where);
        $groupBy=$this->groupBy;
        if($this->groupByTime) $groupBy[]="time($this->groupByTime)";
        if($groupBy) $query[]="GROUP BY ".implode(',',$groupBy);
        if($this->orderBy) $query[]="ORDER BY ".implode(',',$this->orderBy);
        if($this->limitClause) $query[] = $this->limitClause;
        if($this->offsetClause) $query[]= $this->offsetClause;

        return implode(' ', $query);
    }

    public function isValidQuery():bool
    {
        try {
            $this->validate();
        }
        catch(\InvalidArgumentException $e) {
            return false;
        }
        return true;
    }

    public function validate():self
    {
        $err=[];
        if (! $this->measurement) {
            $err[] = 'Measurement is required';
        }
        if (! $this->select) {
            $err[] = 'At least one select field is required';
        }
        if($err) throw new \InvalidArgumentException(implode(', ', $err));
        return $this;
    }

    public function listFieldKeys():array
    {
        if(!$this->measurement) throw new \InvalidArgumentException('Measurement must be set');
        if(is_null($this->fieldKeys)) {
            $this->fieldKeys = $this->db->listFieldKeys($this->measurement);
        }
        return $this->fieldKeys;
    }

    public function listTagKeys():array
    {
        if(!$this->measurement) throw new \InvalidArgumentException('Measurement must be set');
        if(is_null($this->tagKeys)) {
            $this->tagKeys = $this->db->listTagKeys($this->measurement);
        }
        return $this->tagKeys;
    }

    public function getDatabase():Database
    {
        return $this->db;
    }

    public function createDate(int ...$values):string
    {
        if(count($values)<3) throw new \InvalidArgumentException('Year, month, and day must be provided');
        $date=implode('-',array_slice($values, 0, 3));
        if(isset($values[3])) $date.' '.implode(':',array_slice($values, 3, 3));
        return (new \DateTime($date))->format(DATE_RFC3339);
    }

    public function createDateFromTimestamp(int $timestamp):string
    {
        return (new \DateTime())->setTimestamp($timestamp)->format(DATE_RFC3339);
    }

    private function getTimeDigit(string $time):?int
    {
        return ($digit=substr($time, 0, -1)) && $this->isDigit($digit)?(int)$digit:null;
    }
    private function getTimeUnit(string $time):?string
    {
        return ($unit=substr($time, -1)) && isset(self::VALID_DURATIONS[$unit])?$unit:null;
    }
    private function getUnitSeconds(string $unit):?int
    {
        return self::VALID_DURATIONS[$unit]??null;
    }
    private function isDigit($value):bool
    {
        return is_int($value) || ctype_digit($value);
    }
    private function getSeconds($time):?int{
        if($time==0) return 0;
        return ($digit=$this->getTimeDigit($time)) && ($unit=$this->getTimeUnit($time))?$digit*self::VALID_DURATIONS[$unit]:null;
    }
    private function isLiteralTime(string $time):bool{
        return !is_null($this->getSeconds($time));
    }
    private function validateRFCDate(string $date):bool
    {
        return \DateTime::createFromFormat(\DateTime::RFC3339, $date) !== FALSE;
    }
}
