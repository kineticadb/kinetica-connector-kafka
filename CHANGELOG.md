GPUdb Kafka Connector Changelog
===============================

Version 7.0.0.0 - 2019-01-22
----------------------------

-   Version release

Version 6.2.1 - 2019-01-22
--------------------------

-   Added ability to handle nullable columns
-   Added configuration classes for source and sink connectors
-   Added license references, updated project docs and readme file
-   Updated test suite and directory structure
-   Added ability to handle no-key JSON data
-   Added BulkInserter flush before shutdown
-   Separated Kafka datapump helpers into test-connect project
-   Added README.md with detailed connector configurations for all 
possible Kafka message formats and data ingest scenarios

Version 6.2.0 - 2018-04-11
--------------------------

-   Version release
-   Added option dest_table_override
-   Added support for schema-less sink connectors
-   Added enchancements to support Oracle Golden Gate
-   Removed schema part of table name
-   Added type conversion
-   Added option to create missing tables
-   Added support for multiple tables in a single put()
-   Fixed case for out of range dates
-   Added PK Upsert
-   Restructuring repository and adding 

Version 6.1.0 - 2017-10-05
--------------------------

-   Releasing version

Version 6.0.0 - 2017-01-24
--------------------------

-   Releasing version

Version 5.2.0 - 2016-10-19
--------------------------

-   Changed configuration to use ConfigDef
-   Failures are propagated to the framework
-   System test was added

Version 5.2.0 - 2016-07-08
--------------------------

-   Initial version
