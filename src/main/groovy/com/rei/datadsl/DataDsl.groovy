package com.rei.datadsl;

import com.rei.datadsl.reader.DataSetReader
import com.rei.datadsl.reader.DatabaseDataSetReader
import com.rei.datadsl.reader.FlatXmlDataSetReader
import com.rei.datadsl.reader.GroovyClosureDataSetReader
import com.rei.datadsl.reader.GroovyDataSetReader
import com.rei.datadsl.writer.DatabaseDataSetWriter
import com.rei.datadsl.writer.GroovyDataSetWriter

import javax.sql.DataSource

import com.rei.datadsl.writer.SqlDataSetWriter

class DataDsl {

    private List<DataSetReader> readers
    
    static DataDsl from(Closure clos) {
        return new DataDsl(readers: clos ? [new GroovyClosureDataSetReader(clos)] : [])
    }
    
    static DataDsl from(String groovyDataset) {
        return new DataDsl(readers: groovyDataset ? [new GroovyDataSetReader(groovyDataset)] : [])
    }
    
    static DataDsl from(InputStream groovyDataset) {
        return new DataDsl(readers: groovyDataset ? [new GroovyDataSetReader(groovyDataset)] : [])
    }
    
    static DataDsl fromXml(String flatXml) {
        return new DataDsl(readers: flatXml ? [new FlatXmlDataSetReader(flatXml)] : [])
    }
    
    static DataDsl fromXml(InputStream flatXml) {
        return new DataDsl(readers: flatXml ? [new FlatXmlDataSetReader(flatXml)] : [])
    }
    
    static DataDsl from(DataSource ds, Collection<String> tables) {
        return new DataDsl(readers: [new DatabaseDataSetReader(ds, tables)])
    }
    
    DataDsl andFrom(Closure clos) {
        if (clos) {
            readers << new GroovyClosureDataSetReader(clos)
        }
        return this
    }
    
    DataDsl andFrom(String groovyDataset) {
        if (groovyDataset) {
            readers << new GroovyDataSetReader(groovyDataset)
        }
        return this
    }
    
    DataDsl andFrom(InputStream groovyDataset) {
        if (groovyDataset) {
            readers << new GroovyDataSetReader(groovyDataset)
        }
        return this
    }
    
    DataDsl andFromXml(String flatXml) {
        if (flatXml) {
            readers << new FlatXmlDataSetReader(flatXml)
        }
        return this
    }
    
    DataDsl andFromXml(InputStream flatXml) {
        if (flatXml) {
            readers << new FlatXmlDataSetReader(flatXml)
        }
        return this
    }
    
    DataDsl andFrom(DataSource ds, Collection<String> tables) {
        readers << new DatabaseDataSetReader(ds, tables)
        return this
    }
    
    void into(DataSource dest) {
        into(dest, false)
    }

    void into(DataSource dest, boolean clean) {
        readers.each { reader ->
            def writer = new DatabaseDataSetWriter(dest, clean)
            writer.write(reader.read())
        }
    }

    void into(OutputStream dest) {
        readers.each { reader ->
            def writer = new GroovyDataSetWriter(dest)
            writer.write(reader.read())
        }
    }

    void intoSql(OutputStream dest) {
        readers.each { reader ->
            def writer = new SqlDataSetWriter(dest)
            writer.write(reader.read())
        }
    }
}