package com.rei.datadsl.writer


import org.junit.Test

import com.rei.datadsl.reader.GroovyClosureDataSetReader

class SqlDataSetWriterTest {

    @Test
    void canWriteReadableGroovyDataSet() {
        def ds = new GroovyClosureDataSetReader({
            address.id('id')
            people.id('id')
            
            someTable(pattern: /foo\d+/)
            
            100.times { n ->
                def person = people(name: 'joe blow' + n, active: n % 2 == 0, born: new Date())
                address(street: "$n main st".toString(), city: 'seattle', state: 'wa', person: person)
                address(street: "$n oak st".toString(), city: 'seattle', state: 'wa', person: person)    
            }
        }).read()
        
        def out = new StringWriter()
        def writer = new SqlDataSetWriter(out)
        writer.write(ds)
        
        println out.toString()
    }
}
