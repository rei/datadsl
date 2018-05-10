package io.github.phonydata

class DataSet {
    final Map<String, Table> tables
    final List<String> tableOrder

    DataSet(Map<String, Table> tables, List<String> tableOrder) {
        this.tables = tables
        this.tableOrder = tableOrder
    }
}
