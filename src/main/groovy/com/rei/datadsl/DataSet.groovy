package com.rei.datadsl

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DataSet {
    private static final Logger logger = LoggerFactory.getLogger(DataSet.class);

    final Map<String, Table> tables
    final List<String> tableOrder

    DataSet(Map<String, Table> tables, List<String> tableOrder) {
        this.tables = tables.collectEntries { [it.key.toLowerCase(), it.value] }
        this.tableOrder = tableOrder.collect { it.toLowerCase() }
    }

    List<Table> getTablesInTopologicalOrder() {
        return topologicalTableOrder.collect { tables[it] }
    }

    List<String> getTopologicalTableOrder() {
        def fks = tables.values().collectEntries { [it.name.toLowerCase(), it.foreignKeys.collect {fk -> fk.name}] }
        def fallbackOrder = tableOrder.collect() // need a copy to make non-destructive

        def sorted = []
        def counter = fks.size()
        while (!fks.isEmpty()) {
            if (counter-- < 0) {
                logger.warn("unable to completely sort dataset with circular table references: {}, falling back on table order {}",
                            fks, fallbackOrder)
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
}
