<?php

namespace InfluxDB\Driver;

class PlainPhpHttpResponse {

    private $result_headers = [];
    private $body = [];

    function __construct($result_headers, $body) {
        $this->body = $body;
        $this->result_headers = $result_headers;
    }

    function getStatusCode() {
        if( is_array($this->result_headers) ) {
            $a = explode(' ', $this->result_headers[0]);

            if( count($a) > 1 ) {
                return intval($a[1]);
            }
        }

        return 0;
    }

    function getBody() {
        return $this->body;
    }
}
