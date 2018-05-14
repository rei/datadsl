package com.rei.datadsl

class DataSet {
    final Map<String, Table> tables
    final List<String> tableOrder

    DataSet(Map<String, Table> tables, List<String> tableOrder) {
        this.tables = tables.collectEntries { [it.key.toLowerCase(), it.value] }
        this.tableOrder = tableOrder.collect { it.toLowerCase() }
    }
}
