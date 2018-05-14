package com.rei.datadsl.writer

import groovy.sql.Sql

import java.sql.DatabaseMetaData

import javax.sql.DataSource

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.rei.datadsl.DataSet
import com.rei.datadsl.Table

class DatabaseDataSetWriter implements DataSetWriter {
    private static Logger logger = LoggerFactory.getLogger(DatabaseDataSetWriter)
    
    private Sql sql
    private String quote
    private boolean clean
    
    DatabaseDataSetWriter(DataSource dataSource, boolean clean) {
        sql = new Sql(dataSource)
        quote = dataSource.connection.metaData.identifierQuoteString
        this.clean = clean
    }
    
    void write(DataSet ds) {
        def tableNameMappings = getTableNameMappings(ds)
        def fks = findForeignKeys(tableNameMappings, ds.tables.keySet())
        def sortedTables = topologicalSort(fks, ds.tableOrder)

        sql.withTransaction {
            sql.withBatch {
                if (clean) {
                    sortedTables.reverse().each {
                        logger.info("deleting data from table {}", tableNameMappings[it].actualName)
                        sql.executeUpdate('delete from ' + tableNameMappings[it].actualName)
                    }
                }
                
                sortedTables.each { tableName ->
                    sql.cacheStatements { 
                        def table = ds.tables[tableName]

                        def insertStatement = createInsertStatement(table, tableNameMappings[tableName])
                        table.each {
                            sql.executeInsert(insertStatement, it.data);
                        }
                        
                        logger.info("inserted {} rows into {}", table.rows.size(), table.name)
                    }
                }    
            }
        }
    }

    String createInsertStatement(Table t, TableNameMapping nameMapping) {
        def cols = t.columns.collect { nameMapping.columnMappings[it] }
        def quotedCols = cols.collect { q(it) }
        def paramCols = t.columns.collect { ':' + it }
        return "insert into ${q(nameMapping.actualName)} (${quotedCols.join(',')}) values (${paramCols.join(',')});".toString()
    }
    
    private String q(String s) {
        return quote + s + quote
    }
    
    static def topologicalSort(Map fks, List<String> fallbackOrder) {
        def sorted = []
        def counter = fks.size()
        while (!fks.isEmpty()) {
            if (counter-- < 0) {
                logger.warn("unable to completely sort dataset with circular table references: {}, falling back on table order {}", fks, fallbackOrder)
                break
            }
            def unReferenced = fks.findAll { t, refs -> refs.isEmpty() }.keySet()
            sorted.addAll(unReferenced)
            unReferenced.each {
                fks.remove(it)
                fallbackOrder.remove(it)
            }
            fks.each { t, refs -> refs.removeAll(unReferenced) }
        }

        if (!fks.isEmpty()) {
            sorted.addAll(fallbackOrder)
        }

        return sorted
    }

    /**
     * in case table/column names are created with specific casing, map between dataset name and real one
     * @param ds
     * @return
     */
    private Map<String, TableNameMapping> getTableNameMappings(DataSet ds) {
        def metaData = sql.dataSource.connection.metaData
        def tables = metaData.getColumns(null, null, null, null)

        def mappings = [:]

        while (tables.next()) {
            def row = tables.toRowResult()
            def tableName = row['TABLE_NAME'].toLowerCase()
            if (ds.tables.containsKey(tableName)) {
                if (mappings[tableName] == null) {
                    mappings[tableName] = new TableNameMapping(actualName: row['TABLE_NAME'])
                }
                mappings[tableName].columnMappings[row['COLUMN_NAME'].toLowerCase()] = row['COLUMN_NAME']
            }
        }

        return mappings
    }

    private Map findForeignKeys(tableNameMappings, tableNames) {
        def fks = tableNames.collectEntries { [it.toLowerCase(), [] as Set] }
        def metaData = sql.dataSource.connection.metaData
        tableNames.each { addFksForTable(metaData, tableNameMappings[it].actualName, fks) }
        return fks
    }
    
    private static void addFksForTable(DatabaseMetaData metaData, tableName, fks) {
        def rs = metaData.getExportedKeys(null, null, tableName)
        
        while (rs.next()) {
            def dependency = rs.toRowResult()['FKTABLE_NAME'].toLowerCase()
            if (fks[dependency] == null) {
                logger.debug("skipping fk reference to unknown table {} -> {}", tableName, dependency) 
                continue 
            }
            fks[dependency].add(tableName.toLowerCase())
        }
    }

    private static class TableNameMapping {
        String actualName
        Map<String, String> columnMappings = [:]
    }
}
