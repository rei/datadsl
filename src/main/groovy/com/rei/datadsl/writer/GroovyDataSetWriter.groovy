package com.rei.datadsl.writer

import groovy.json.StringEscapeUtils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.rei.datadsl.DataSet
import com.rei.datadsl.Row
import com.rei.datadsl.Table


class GroovyDataSetWriter implements DataSetWriter {
    private Logger logger = LoggerFactory.getLogger(getClass())
    
    private PrintWriter out;

    GroovyDataSetWriter(OutputStream out) {
        this.out = out.newPrintWriter()
    }
    
    GroovyDataSetWriter(Writer out) {
        this.out = out.newPrintWriter();
    }
    
    public void write(DataSet ds) {
        def totalRows = 0
        
        ds.tables.values().each { Table t ->
            t.rows.each { Row r ->
                def cols = r.data.findAll { it.value != null }
                                 .collect { n, v -> "${n}:${toValue(v)}" }.join(',')
                out.println("${t.name}(${cols})")
            }
            totalRows += t.rows.size()
            
            out.println()
        }
        
        out.flush()
        logger.info("wrote {} tables with {} total rows to output", ds.tables.size(), totalRows)        
    }
    
    private String toValue(Object o) {
        switch (o) {
            case Number: return o.toString()
            case CharSequence: return "\"${sanitize(o)}\""
            case Boolean: return o.toString() 
            case Date: return "new Date(${o.time})"
        }
    }
    
    private String sanitize(String input) {
        return StringEscapeUtils.escapeJava(input).replace('$', '\\$')
    }
}
