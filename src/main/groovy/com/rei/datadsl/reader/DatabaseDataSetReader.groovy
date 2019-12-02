package com.rei.datadsl.reader

import groovy.sql.Sql
import com.rei.datadsl.DataSet
import com.rei.datadsl.Table

import java.sql.ResultSet

import javax.sql.DataSource

import com.rei.datadsl.db.DatabaseDataSetHandler

class DatabaseDataSetReader extends DatabaseDataSetHandler implements DataSetReader {

    private Set<String> tablesToImport
    
    DatabaseDataSetReader(DataSource ds) {
        super(new Sql(ds))
    }
    
    DatabaseDataSetReader(DataSource ds, Collection<String> tablesToImport) {
        super(new Sql(ds))
        this.tablesToImport = new HashSet(tablesToImport)
    }
    
    DataSet read() {
        if (!tablesToImport) {
            tablesToImport = findAllTables()
        }
        
        def queries = tablesToImport.collectEntries { [it, "select * from $it".toString()] }
        
        def dataSet = new DataSet([:], tablesToImport as List)
        
        sql.withBatch {
            queries.each { tableName, query ->
                Table table = new Table(name: tableName)
                
                sql.rows(query).each {
                    table.addRow(it)
                }
                
                dataSet.tables[tableName] = table
            }
        }

        discoverMetadata(dataSet)

        return dataSet;
    }
    
    private Set<String> findAllTables() {
        def tables = [] as Set
        sql.cacheConnection {conn ->
            ResultSet rs = conn.metaData.getTables(conn.catalog, conn.schema, "%", null);

            while (rs.next()) {
                tables << rs.getString(3)
            }

        }
        return tables
    }
}
