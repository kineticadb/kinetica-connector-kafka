# GPUdb Kafka Connector Changelog

## Version 7.1

### Version 7.1.1.2 -- 2020-11-06

#### Uptated

-   Deprecated parameters description in KineticaSinkConnectorConfig
    for the ease of use in Confluent Connect UI.

### Version 7.1.1.1 -- 2020-10-30

#### Added

-   Kafka v2.6 and Confluent v6.0.0-ce support.

### Version 7.1.1.0 -- 2020-08-25

#### Added

-   `kinetica.enable_multihead` option to turn on
    multihead data ingest for Sink Connector (default true).

### Version 7.1.0.0 -- 2020-05-27

#### Changed
-   Sink Connector configuration parameters names to allow grouping
    by scope and relevance. Old parameter names are deprecated.
    The following parameters were renamed:
    `kinetica.create_table` into `kinetica.tables.create_table`,
    `kinetica.table_prefix` into `kinetica.tables.prefix`,
    `kinetica.collection_name` into `kinetica.tables.schema_name`,
    `kinetica.dest_table_override` into `kinetica.tables.destination_name`,
    `kinetica.single_table_per_topic` into `kinetica.tables.single_table_per_topic`,
    `kinetica.update_on_existing_pk` into `kinetica.tables.update_on_existing_pk`,
    `kinetica.allow_schema_evolution` into `kinetica.schema_evolution.enabled`,
    `kinetica.add_new_fields_as_columns` into `kinetica.schema_evolution.add_new_fields_as_columns`,
    `kinetica.make_missing_field_nullable` into `kinetica.schema_evolution.make_missing_field_nullable`.

-   Referencing table by using [schema_name.]table_name format
    to support Kinetica 7.1 table naming restrictions.
    KineticaSourceConnector uses this naming format in
    `kinetica.table_names` configuration parameter.
    KineticaSinkConnector uses `kinetica.collection name`
    for `schema_name` and other configuration parameters
    to derive `table_name`.


## Version 7.0

### Version 7.0.2.0 -- 2020-04-28

#### Updated

-   Kafka version support to 2.4.1 version.

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
