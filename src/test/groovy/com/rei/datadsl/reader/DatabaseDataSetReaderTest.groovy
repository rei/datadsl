package com.rei.datadsl.reader;

import static org.junit.Assert.*
import groovy.sql.Sql

import javax.sql.DataSource

import org.h2.jdbcx.JdbcConnectionPool
import org.junit.Test

class DatabaseDataSetReaderTest {

    @Test
    void canParseDataSet() {
        def dataSource = createDataSource()
        
        def sql = new Sql(dataSource)
        sql.executeUpdate("create table people(id int primary key, name varchar(255))")
        sql.executeUpdate("create table address(id int primary key, street varchar(255), city varchar(255), state varchar(2), person int)")
        sql.executeUpdate("alter table address add foreign key (person) references people(id)")
        
        sql.withBatch {
            100.times { n ->
                sql.executeInsert("insert into people (id, name) values (${n+1}, ${'joe blow' + n})")
                sql.executeInsert("insert into address (id, street, city, state, person) values (${n+1}, ${n + ' main st'}, 'seattle', 'wa', ${n+1})")
            }
        }
        
        def reader = new DatabaseDataSetReader(dataSource, ['people', 'address'])
        def ds = reader.read()

        ds.tables.people.each {
            println it
        }
                
        assertEquals(2, ds.tables.size())
        assertEquals(100, ds.tables.people.size())
        assertEquals(100, ds.tables.address.size())
        assertEquals(1, ds.tables['address'].foreignKeys.size())
        assertEquals(ds.tables['people'], ds.tables['address'].foreignKeys.first())

    }
    
    private DataSource createDataSource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1", "sa", "sa").dataSource
    }
}
