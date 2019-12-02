package com.rei.datadsl.writer

import groovy.sql.Sql

import java.sql.DatabaseMetaData

import javax.sql.DataSource

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.rei.datadsl.DataSet
import com.rei.datadsl.db.DatabaseDataSetHandler
import com.rei.datadsl.Table

class DatabaseDataSetWriter extends DatabaseDataSetHandler implements DataSetWriter {
    private static Logger logger = LoggerFactory.getLogger(DatabaseDataSetWriter)
    
    private String quote
    private boolean clean
    
    DatabaseDataSetWriter(DataSource dataSource, boolean clean) {
        super(new Sql(dataSource))
        quote = dataSource.connection.metaData.identifierQuoteString
        this.clean = clean
    }
    
    void write(DataSet ds) {
        discoverActualNames(ds)
        discoverForeignKeys(ds)
        def sortedTables = ds.topologicalTableOrder

        sql.withTransaction {
            sql.withBatch {
                if (clean) {
                    sortedTables.reverse().each {
                        logger.info("deleting data from table {}", ds.tables[it].actualName)
                        sql.executeUpdate('delete from ' + ds.tables[it].actualName)
                    }
                }
                
                sortedTables.each { tableName ->
                    sql.cacheStatements { 
                        def table = ds.tables[tableName]

                        def insertStatement = createInsertStatement(table)
                        table.each {
                            sql.executeInsert(insertStatement, it.data);
                        }
                        
                        logger.info("inserted {} rows into {}", table.rows.size(), table.name)
                    }
                }    
            }
        }
    }

    String createInsertStatement(Table t) {
        def cols = t.actualColumnNames.values()
        def quotedCols = cols.collect { q(it) }
        def paramCols = t.columns.collect { ':' + it }
        return "insert into ${q(t.actualName)} (${quotedCols.join(',')}) values (${paramCols.join(',')});".toString()
    }
    
    private String q(String s) {
        return quote + s + quote
    }
}
