<?php

    namespace InfluxDB\Test\unit\Query;

    use InfluxDB\Query\Builder;
    use InfluxDB\Test\unit\AbstractTest;

    class BuilderTest extends AbstractTest
    {
        /**
         * @var Builder
         */
        protected $queryBuilder;

        public function setUp(): void
        {
            parent::setUp();

            $this->queryBuilder = $this->database->getQueryBuilder();
        }

        /**
         * @return array
         */
        public function provideLimit()
        {
            return [
                ['test_metric', 2],
            ];
        }

        /**
         * @dataProvider provideLimit
         * @param $from
         * @param $limit
         */
        public function testLimit($from, $limit)
        {
            $this->assertEquals(
                sprintf('SELECT * FROM "%s" LIMIT %s', $from, $limit),
                $this->database->getQueryBuilder()
                    ->from($from)
                    ->limit($limit)
                    ->getQuery()
            );
        }

        /**
         * @return array
         */
        public function provideOffset()
        {
            return [
                ['test_metric', 2],
            ];
        }

        /**
         * @dataProvider provideOffset
         * @param $offset
         * @param $from
         */
        public function testOffset($from, $offset)
        {
            $this->assertEquals(
                sprintf('SELECT * FROM "%s" OFFSET %s', $from, $offset),
                $this->database->getQueryBuilder()
                    ->from($from)
                    ->offset($offset)
                    ->getQuery()
            );
        }
    }