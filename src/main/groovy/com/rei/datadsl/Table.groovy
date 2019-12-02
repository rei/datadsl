package com.rei.datadsl


class Table implements Iterable<Row> {
    String name
    private String actualName
    private String idColumn
    private long nextId = -1
    private Set<Table> foreignKeys = [] as Set
    private Map<String, Object> defaults = [:]
    
    private final List<Row> rows = []
    private final Set<String> columns = [] as Set
    private Map<String, String> actualColumnNames = [:]

    List<Row> getRows() {
        return rows
    }
    
    Row addRow(Map<String, Object> row) {
        columns.addAll(row.keySet())
        
        if (!row[idColumn] && nextId > 0) {
            row[idColumn] = nextId++
        } else if (row[idColumn] && row[idColumn] > nextId) {
            nextId = row[idColumn]+1
        }

        foreignKeys.addAll(row.values().findAll { it instanceof Row }.collect { it.table })

        def r = new Row(table: this, data: row)
        rows.add(r)
        return r
    }
    
    void defaults(Map<String, Object> defaults) {
        this.defaults = defaults
        this.columns.addAll(defaults.keySet())
    }
    
    void id(String col, boolean auto = true) {
        idColumn = col
        columns.add(col)
        
        if (auto) {
            nextId = 1
        }
    }

    void actualTableName(String name) {
        this.actualName = name
    }

    String getActualName() {
        return actualName ?: name
    }

    void actualColumnNames(Collection<String> names) {
        this.actualColumnNames = names.collectEntries { [it.toLowerCase(), it] }
    }

    void actualColumnName(String name, String actualName) {
        actualColumnNames[name] = actualName
    }

    Map<String, String> getActualColumnNames() {
        return actualColumnNames.isEmpty() ? columns.collectEntries { [it, it] } : actualColumnNames
    }

    void fk(Table table) {
        foreignKeys.add(table)
    }

    Set<Table> getForeignKeys() {
        return foreignKeys
    }

    Map<String, Object> getDefaults() { defaults }

    Iterator<Row> iterator() {
        return rows.iterator()
    }
    
    int size() {
        return rows.size()
    }
}

class Row {
    Table table
    Map<String, Object> data
    
    Set<String> getColumns() { table.columns } 
    def propertyMissing(name) { val(data.containsKey(name) ? data[name] : table.defaults[name]) }
    
    def propertyMissing(name, value) { 
        data[name] = value
        columns.add(name) 
    }
    
    Map<String, Object> getData() {
        return columns.collectEntries { [it, propertyMissing(it)] }
    }

    Map<String, Object> getDataWithActualColumnNames() {
        return columns.collectEntries { [table.actualColumnNames[it], propertyMissing(it.toLowerCase())] }
    }

    private def val(v) {
        if (v instanceof Row) {
            if (v.table == null || v.table.idColumn == null) {
                throw new IllegalStateException('table with idColumn must be set to pass row directly')
            }
            return v.data[v.table.idColumn]
        }
        
        if (v instanceof Closure) {
            return v()
        }
        
        return v
    }
    
    @Override
    String toString() {
        return getData().toString();
    }
}