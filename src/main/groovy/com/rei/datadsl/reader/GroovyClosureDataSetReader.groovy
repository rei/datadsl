package com.rei.datadsl.reader;

import com.rei.datadsl.DataSet
import com.rei.datadsl.Table

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class GroovyClosureDataSetReader implements DataSetReader {
    private Logger logger = LoggerFactory.getLogger(getClass())

    private Closure closure

    GroovyClosureDataSetReader(Closure clos) {
        this.closure = clos
    }

    DataSet read() {
        def tables = parse(closure)
        return new DataSet(tables, tables.keySet() as List)
    }

    private Map<String, Table> parse(Closure clos) {
        def delegate = new Delegate()
        clos.delegate = delegate
        clos()

        logger.info ("read groovy closure dataset with {} tables and {} total rows",
            delegate.tables.size(), delegate.tables.values().sum { it.rows.size() })

        return delegate.tables
    }
}

class Delegate {
    def tables = new LinkedHashMap()

    def getTable(name) {
        if (!tables[name]) {
            tables[name] = new Table(name: name)
        }
        return tables[name]
    }

    def methodMissing(String name, args) {
        if (args.size() > 1 || !(args[0] instanceof Map)) {
            throw new IllegalArgumentException('may only pass a single map to row method!')
        }
        return getTable(name).addRow(args[0])
    }

    def propertyMissing(String name) { getTable(name) }
}