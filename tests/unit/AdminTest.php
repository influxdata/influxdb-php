<?php

namespace InfluxDB\Test\unit;

use InfluxDB\Client;
use InfluxDB\Client\Admin;
use InfluxDB\ResultSet;

class AdminTest extends AbstractTest
{
    public function testCreateUser()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminMock = new Client\Admin($clientMock);

        $resultSet = $adminMock->createUser('test', 'password');

        $this->assertEquals('CREATE USER test WITH PASSWORD \'password\'', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testCreateUserWithPrivilege()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminMock = new Client\Admin($clientMock);

        $resultSet = $adminMock->createUser('test', 'password', Client\Admin::PRIVILEGE_ALL);

        $this->assertEquals('CREATE USER test WITH PASSWORD \'password\' WITH ALL PRIVILEGES', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testChangeUserPassword()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminMock = new Client\Admin($clientMock);

        $resultSet = $adminMock->changeUserPassword('test', 'password', Client\Admin::PRIVILEGE_ALL);

        $this->assertEquals('SET PASSWORD FOR test = \'password\'', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testShowUsers()
    {
        $capturedQueryArgs = [];
        $resultJson = file_get_contents(__DIR__ . '/json/result-test-users.example.json');
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminMock = new Client\Admin($clientMock);

        $resultSet = $adminMock->showUsers();

        $this->assertEquals('SHOW USERS', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testDropUser()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminClient = new Client\Admin($clientMock);

        $resultSet = $adminClient->dropUser('smith');

        $this->assertEquals('DROP USER smith', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testGrantWithDatabase()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminClient = new Client\Admin($clientMock);

        $resultSet = $adminClient->grant(Admin::PRIVILEGE_READ, 'smith', 'example_db');

        $this->assertEquals('GRANT READ ON example_db TO smith', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testRevokeWithDatabase()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminClient = new Client\Admin($clientMock);

        $resultSet = $adminClient->revoke(Admin::PRIVILEGE_READ, 'smith', 'example_db');

        $this->assertEquals('REVOKE READ ON example_db FROM smith', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testRevokeWithoutDatabase()
    {
        $capturedQueryArgs = [];
        $resultJson = '{}';
        $clientMock = $this->getMockClient($resultJson, $capturedQueryArgs);
        $adminClient = new Client\Admin($clientMock);

        $resultSet = $adminClient->revoke(Admin::PRIVILEGE_ALL, 'smith', null);

        $this->assertEquals('REVOKE ALL PRIVILEGES FROM smith', $capturedQueryArgs['query']);

        $this->assertNull($capturedQueryArgs['database']);
        $this->assertEquals([], $capturedQueryArgs['parameters']);
        $this->assertEquals($resultJson, $resultSet->getRaw());
    }

    public function testRevokeInvalidPrivilege()
    {
        $clientMock = $this->getClientMock();
        $adminClient = new Client\Admin($clientMock);

        $this->expectException(\InfluxDB\Client\Exception::class);
        $this->expectExceptionMessage('does not exists is not a valid privileges, allowed privileges: READ, WRITE, ALL');
        $adminClient->revoke('does not exists', 'smith', 'example_db');
    }

    public function testRevokeAllWithoutGivingDatabase()
    {
        $clientMock = $this->getClientMock();
        $adminClient = new Client\Admin($clientMock);

        $this->expectException(\InfluxDB\Client\Exception::class);
        $this->expectExceptionMessage('Only grant ALL cluster-wide privileges are allowed');
        $adminClient->revoke(Admin::PRIVILEGE_READ, 'smith', null);
    }

    private function getMockClient($resultJson, &$capturedQueryArgs = [])
    {
        $clientMock = $this->getClientMock();

        $clientMock->expects($this->once())
            ->method('query')
            ->willReturnCallback(function ($database, $query, $parameters) use ($resultJson, &$capturedQueryArgs) {
                $capturedQueryArgs['database'] = $database;
                $capturedQueryArgs['query'] = $query;
                $capturedQueryArgs['parameters'] = $parameters;

                return new ResultSet($resultJson);
            });

        return $clientMock;
    }

}