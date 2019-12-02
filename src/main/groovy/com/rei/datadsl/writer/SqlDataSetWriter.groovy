package com.rei.datadsl.writer

import java.util.function.Function

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.rei.datadsl.DataSet
import com.rei.datadsl.Row
import com.rei.datadsl.Table

class SqlDataSetWriter implements DataSetWriter {
    private static final DATE_FORMAT = 'yyyyMMddHHddmmssSSSS'
    private Logger logger = LoggerFactory.getLogger(getClass())

    private PrintWriter out;

    Function<Date, String> dateHandler = { date ->
        return "PARSEDATETIME('${date.format(DATE_FORMAT)}', '${DATE_FORMAT}')"
    }

    Function<Boolean, String> booleanHandler = { o -> o.toString() }

    SqlDataSetWriter(OutputStream out) {
        this.out = out.newPrintWriter()
    }

    SqlDataSetWriter(Writer out) {
        this.out = out.newPrintWriter();
    }

    void write(DataSet ds) {
        def totalRows = 0

        ds.tablesInTopologicalOrder.each { Table t ->
            t.rows.each { Row r ->
                out.println(createInsertStatement(r))
            }
            totalRows += t.rows.size()
            
            out.println()
        }
        
        out.flush()
        logger.info("wrote {} tables with {} total rows to output", ds.tables.size(), totalRows)        
    }

    String createInsertStatement(Row r) {
        def cols = r.table.actualColumnNames.values()
        def data = r.dataWithActualColumnNames
        def vals = r.table.actualColumnNames.values().collect { toValue(data[it]) }
        return "insert into ${r.table.actualName} (${cols.join(',')}) values (${vals.join(',')});".toString()
    }

    private String toValue(Object o) {
        if (o == null) {
            return 'NULL'
        }
        switch (o) {
            case Number: return o.toString()
            case Boolean: return booleanHandler.apply(o)
            case Date: return dateHandler.apply(o)
        }
        return "'${o.toString().replace("'", "''")}'"
    }
}
