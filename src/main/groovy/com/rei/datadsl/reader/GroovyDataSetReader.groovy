package com.rei.datadsl.reader

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.rei.datadsl.DataSet
import com.rei.datadsl.Table


class GroovyDataSetReader implements DataSetReader {
    private static final int MAX_SCRIPT_SIZE = 1024 * 60 // leave some headroom to 64k method body limit
    
    private Logger logger = LoggerFactory.getLogger(getClass())
    
    private InputStream input;

    GroovyDataSetReader(String text) {
        this(new ByteArrayInputStream(text.bytes))
    }
    
    GroovyDataSetReader(InputStream input) {
        this.input = input
    }
    
    DataSet read() {
        def tables = parse(input)
        return new DataSet(tables, tables.keySet() as List)
    }
        
    private Map<String, Table> parse(InputStream input) {
        
        def tables = new LinkedHashMap()
        def lines = new LinkedList(input.readLines())
        
        while (!lines.empty) {
            def sb = new StringBuilder()
            while (!lines.empty && (sb.size() + lines.head().size()) < MAX_SCRIPT_SIZE) {
                sb.append(lines.head()).append('\n')
                lines.remove(0)
            }
            execScript(sb.toString(), tables)
        }
        
        logger.info ("read groovy dataset with {} tables and {} total rows", tables.size(), tables.values().sum { it.rows.size() })
        
        return tables
    }
    
    private static void execScript(text, tables) {
        GroovyShell shell = new GroovyShell()
        def script = shell.parse(text)
        
        def getTable = { name ->
            if (!tables[name]) {
                tables[name] = new Table(name: name)
            }
            return tables[name]
        }
        
        script.metaClass.methodMissing = { name, args ->
            if (args.size() > 1 || !(args[0] instanceof Map)) {
                throw new IllegalArgumentException('may only pass a single map to row method!')
            }
            return getTable(name).addRow(args[0])
        }
        
        script.metaClass.propertyMissing = { getTable(it) }
        
        script.run()
    }
}