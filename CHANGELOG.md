# GPUdb Kafka Connector Changelog

## Version 7.0

### Version 7.0.1.3 -- 2019-05-07

#### Changed

-   /alter/table endpoint to /alter/table/columns endpoint 
for performance improvement on table altering operations.
-   Updated the dependent Kinetica Java API version to 7.0.3.0
    to get the latest high availability support related changes.

### Version 7.0.1.2 -- 2019-05-01

#### Added

-   option to support UPDATE of existing records on
    PK match or INSERT new data only (UPSERT).

### Version 7.0.1.1 - 2019-04-01

####   Fixed 

-   datetime type conversion (on ingest format Long timestamp 
to String datetime)


## Version 7.0.0.0 - 2019-01-31

-   Version release


### Version 6.2.1 -- 2019-01-22

#### Added

-   ability to handle nullable columns
-   configuration classes for source and sink connectors
-   license references, updated project docs and readme file
-   ability to handle no-key JSON data
-   BulkInserter flush before shutdown
-   test-connect project for Kafka datapump helper classes 
-   separate README.md file with detailed connector configurations 
for different Kafka message formats and data ingest scenarios


### Version 6.2.0 -- 2018-04-11

#### Added

-   option dest_table_override
-   support for schema-less sink connectors
-   enchancements to support Oracle Golden Gate
-   type conversion
-   option to create missing tables
-   support for multiple tables in a single put()
-   Bulkinserter configuration for PK Upsert

#### Fixed
-   case for out of range dates


### Version 6.1.0 -- 2017-10-05

-   Releasing version

### Version 6.0.0 -- 2017-01-24

-   Releasing version

### Version 5.2.0 -- 2016-10-19

#### Changed 

-   configuration to use ConfigDef
-   failure propagation to the framework

#### Added

-   System tests

### Version 5.2.0 -- 2016-07-08

-   Initial version

