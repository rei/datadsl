DataDSL
=======

DataDSL is a library to import and export test datasets for things like unit testing databases or local dev data.

Features
--------

  * Clear, consise groovy based DSL to define datasets
  * Can both import and export data
  * Automatically escapes sql keywords
  * Automatically topologically sorts tables with foreign keys (falls back on table declaration order within dataset)
  * Auto column sensing
  * Specify default column values
  * ID auto generation
  * Row references
  * Define dataset inline in groovy tests
  * Concise builder pattern to import/export data
  * Reads DBUnit FlatXml datasets for compatibility
  * Automatically detects column casing of quoted column and tables

DSL Overview
------------
  * call an undefined method or reference a property to specify a table
  * Groovy map syntax to pass column values `my_table(id:1, column1: 'test')`
  * Specify an ID column to enable row references or auto id incrementing `my_table.id('id')`
  * Specify defaults column values `my_table.defaults(some_column:'default value', column1: 'default')`

DSL Example
-----------

    // specify id columns
    address.id('id')
    people.id('id')

    // specify columns that will be the same for all rows
    people.defaults(birthday: new Date())

    people(name: 'Jane Doe')

    100.times { n ->
        def person = people(name: 'joe blow' + n) // returns reference to row which will be de-referenced by id

        address(street: "$n main st".toString(), city: 'seattle', state: 'wa',
                person: person /* holds actual value of person.id */)

        address(street: "$n oak st".toString(), city: 'seattle', state: 'wa', person: person)
    }

Usage Example
-------------

    DataSource datasource = ... // get reference to JDBC DataSource
    def dataset = getClass().classLoader.getResourceAsStream('/path/to/dataset') // may also be a String
    DataDsl.from(dataset).into(datasource) // reads data from groovy dataset and writes to datasource

    // reads the data from specified tables and writes a groovy dataset to output stream
    DataDsl.from(datasource, ['table1','table2']).into(new File('/some/file').newOutputStream())


Inline Groovy Example
---------------------

     @Before
     public void setup() {
        DataDsl.from {
            people(name: 'Jane Doe')
            address(street: '123 main st')
        }.into(dataSource)
    }

Get It!
-------

Add this dependency to your project:

    <dependency>
       <groupId>com.rei.datadsl</groupId>
       <artifactId>datadsl</artifactId>
       <version>1.0</version>
    </dependency>

NOTE:

This is a fork/rebrand of https://github.com/jeffskj/phonydata by the original author. 