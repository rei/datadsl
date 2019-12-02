package com.rei.datadsl;

import static org.junit.Assert.*
import groovy.sql.Sql

import javax.sql.DataSource

import org.h2.jdbcx.JdbcConnectionPool
import org.junit.Test

class DataDslTest {

    @Test
    void testFromClosure() {
        assertDataWritten(DataDsl.from { table1(col1:'blah') }.andFrom((String)null))
    }

    @Test
    void testFromString() {
        assertDataWritten(DataDsl.from("table1(col1:'blah')").andFrom { table1(col2: 'blah') })
    }

    @Test
    void testFromInputStream() {
        assertDataWritten(DataDsl.from(new ByteArrayInputStream("table1(col1:'blah')".bytes)))
    }

    @Test
    void testFromXmlString() {
        assertDataWritten(DataDsl.fromXml('<dataset><table1 col="val"/></dataset>'))
    }

    @Test
    void testFromXmlInputStream() {
        assertDataWritten(DataDsl.fromXml(new ByteArrayInputStream('<dataset><table1 col="val"/></dataset>'.bytes)))
    }

    @Test
    void canReadFromDb() {
        def ds = createDataSource()
        def sql = new Sql(ds)
        sql.executeUpdate("create table testing(col varchar(255))")
        sql.executeInsert("insert into testing (col) values ('value')")
        assertDataWritten(DataDsl.from(ds, ['testing']))
    }

    @Test
    void canWriteToDb() {
        def ds = createDataSource()
        def sql = new Sql(ds)
        sql.executeUpdate("create table testing(col varchar(255))")
        DataDsl.from { testing(col: 'val') }.into(ds)
        assertEquals(1, sql.rows('select * from testing').size())
    }

    private void assertDataWritten(DataDsl pd) {
        def out = new ByteArrayOutputStream()
        pd.into(out)
        println out
        assertTrue(out.size() > 0)
    }
    
    private DataSource createDataSource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1", "sa", "sa").dataSource
    }
}
