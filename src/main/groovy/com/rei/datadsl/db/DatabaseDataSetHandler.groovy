package com.rei.datadsl.db

import java.sql.DatabaseMetaData

import groovy.sql.Sql

import com.rei.datadsl.DataSet
import com.rei.datadsl.Table

abstract class DatabaseDataSetHandler {
    Sql sql

    DatabaseDataSetHandler(Sql sql) {
        this.sql = sql
    }

    void discoverMetadata(DataSet ds) {
        discoverActualNames(ds)
        discoverForeignKeys(ds)
    }

    private void discoverActualNames(DataSet ds) {
        def metaData = sql.dataSource.connection.metaData

        def tablesMetadata = metaData.getColumns(null, null, null, null)
        while (tablesMetadata.next()) {
            def row = tablesMetadata.toRowResult()
            def tableName = row['TABLE_NAME'].toLowerCase()
            Table table = ds.tables[tableName]
            if (table != null) {
                table.actualTableName(row['TABLE_NAME'])
                table.actualColumnName(row['COLUMN_NAME'].toLowerCase(), row['COLUMN_NAME'])
            }
        }
    }

    private void discoverForeignKeys(DataSet ds) {
        def metaData = sql.dataSource.connection.metaData
        ds.tables.values().each { addFksForTable(metaData, it, ds) }
    }

    private static void addFksForTable(DatabaseMetaData metaData, Table table, DataSet ds) {
        def rs = metaData.getExportedKeys(null, null, table.actualName)

        while (rs.next()) {
            def dependencyTable = ds.tables[rs.toRowResult()['FKTABLE_NAME'].toLowerCase()]
            if (dependencyTable != null) {
                dependencyTable.fk(table)
            }
        }
    }
}
