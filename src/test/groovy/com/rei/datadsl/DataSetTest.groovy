package com.rei.datadsl

import static org.junit.Assert.assertEquals

import org.junit.Test

class DataSetTest {

    @Test
    void canTopologicalSort() {
        def sorted = topologicalSort(a: [], b: [], c:[], ['b', 'c', 'a'])
        assertEquals(['b', 'c', 'a'], sorted)

        sorted = topologicalSort(a: ['b'], b: [], c:[], ['b', 'c', 'a'])
        assertEquals('b', sorted.first())

        sorted = topologicalSort(a: ['b'], b: ['c'], c:[], ['b', 'c', 'a'])
        assertEquals(['c', 'b', 'a'], sorted)

        sorted = topologicalSort(a: [], b: ['c', 'a'], c:['a'], ['b', 'c', 'a'])
        assertEquals(['a', 'c', 'b'], sorted)
    }

    @Test
    void canHandleCycles() {
        // falls back to declaration order if cycle exists
        def sorted = topologicalSort(a: [], b: ['a'], c: ['d'], d:['c'], ['b', 'a', 'c', 'd'])
        assertEquals(['a', 'b', 'c', 'd'], sorted)
    }

    static def topologicalSort(Map<String, List> fks, List<String> tableOrder) {
        DataSet ds = new DataSet(tableOrder.collectEntries {[it, new Table(name: it)]}, tableOrder)

        fks.each { tableName, tableFks ->
            tableFks.each { fk -> ds.tables[tableName].fk(ds.tables[fk]) }
        }

        return ds.topologicalTableOrder
    }
}
