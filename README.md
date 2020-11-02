[KafkaConnector.build]: <https://github.com/kineticadb/kinetica-connector-kafka/releases>

# Kinetica Kafka Connector

The following guide provides step by step instructions how to build and integrate
Kinetica connector with Kafka platform. It can be integrated as a part of Kafka
stack or Confluent platform. This connector is available as a prebuilt package
release to be downloaded from the [release page][KafkaConnector.build] or can be
built locally from source.

## Contents

* [Installation & Configuration](#installation-configuration)
* [Kinetica Kafka Connector Plugin Deployment](#kinetica-kafka-connector-plugin-deployment)
* [Streaming Data from Kinetica into Kafka](#streaming-data-from-kinetica-into-kafka)
* [Streaming Data from Kafka into Kinetica](#streaming-data-from-kafka-into-kinetica)
* [Configuring Kafka Connector for Secure Connections](#configuring-kafka-connector-for-secure-connections)
* [Connector Error Handling](#connector-error-handling)
* [System Test](#system-test)

This project aims to make Kafka topics accessible to Kinetica, meaning data
can be streamed from a Kinetica table or to a Kinetica table via **Kafka Connect**.
The custom **Kafka Source Connector** and **Kafka Sink Connector** do no additional
processing. (There is a separate set of parameters in `connect-standalone.properties`
that allows lightweight message-at-a-time data transformations, such as create a
new column from Kafka message timestamp. It’s a part of worker task configuration
that’s configured as a part of Kafka Connect.)

The two connector classes that integrate **Kinetica** with **Kafka** are:
* `com.kinetica.kafka.KineticaSourceConnector` - A *Kafka Source Connector*, which
  receives a data stream from the database via table monitor
* `com.kinetica.kafka.KineticaSinkConnector` - A *Kafka Sink Connector*, which
  receives a data stream from a *Kafka Source Connector* and writes it to the database

## Resources

* [Connector GitHub repository][REPOSITORY]
* [Kafka Developer Manual][MANUAL]
* [Confluent Kafka Connect][KAFKA]

[REPOSITORY]: <https://github.com/kineticadb/kinetica-connector-kafka>
[MANUAL]: <https://www.kinetica.com/docs/7.1/connectors/kafka_guide.html>
[KAFKA]: <https://docs.confluent.io/current/connect/index.html>
[AVRO_SCHEMA_EVOLUTION]: <https://avro.apache.org/docs/1.8.1/spec.html>
[KAFKA_SCHEMA_EVOLUTION]: <https://docs.confluent.io/current/schema-registry/docs/avro.html>
[MANAGING_KAFKA]: <ManagingConnector.md>
[KAFKA_SECURITY]: <https://docs.confluent.io/current/security/index.html>


<a name="installation-configuration"></a>
## Installation & Configuration
The connector provided in this project assumes launching will be done on a server
capable of running Kinetica Kafka connectors in standalone mode or to a cluster.

### Version support

Kinetica Kafka connector has a property parameter in the `pom.xml` properties to
set **Kafka** version. Connector build process would add **Kafka** version to
the jar name for easy reference:
`kafka-2.6.0-connector-kinetica-7.1.x.y-jar-with-dependencies.jar`.
Connector code is Java 7 compatible and does not require a separate build to
support Java 8 environment.

**Kafka Connect** allows you to configure the Kinetica Kafka Connector exactly
the same for plain **Kafka** stack or integrated **Confluent** platform. The
following table is provided for the ease of identifying compatible dependency
versions. Based on Kafka version or Confluent platform version, edit `pom.xml`
project properties to build connector compatible with your **Kafka** version.

| Confluent Platform | Apache Kafka | Java version |
| :--- | :--- | :--- |
| 3.1.x | 0.10.1.x | 1.7.0_60, 1.8.0_60 |
| 3.2.x | 0.10.2.x | 1.7.0_60, 1.8.0_60 |
| 3.3.x | 0.11.0.x | 1.7.0_60, 1.8.0_60 |
| 4.0.x | 1.0.x | 1.7.0_60, 1.8.0_60 |
| 4.1.x | 1.1.x | 1.7.0_60, 1.8.0_60 |
| 5.0.x | 2.0.x | 1.8.0_60 |
| 5.1.x | 2.1.x | 1.8.0_60 |
| 5.2.x | 2.2.x | 1.8.0_60 |
| 5.4.1 | 2.4.1 | 1.8.0_60 |
| 6.0.0 | 2.6.x | 1.8.0_60 |

The prebuilt package release for Kinetica Kafka connector can be downloaded from
the [release page][KafkaConnector.build]. Release build name includes the Kafka
and Kinetica versions:
`kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar`.
It can also be built locally from source to match your specific Kafka and Kinetica
versions.

Clone and build the project as follows:

```sh
git clone https://github.com/kineticadb/kinetica-connector-kafka
cd kinetica-connector-kafka/kafka-connect-kinetica
mvn clean compile package -DskipTests=true
```

To run JUnit tests as part of build process, make sure that you have a running
Kinetica instance, and that its URL and user credentials are properly configured
in files `config/quickstart-kinetica-sink.properties`
and `config/quickstart-kinetica-source.properties`, then run

```sh
mvn clean compile package
```

Three JAR files are produced by the Maven build in `kinetica-connector-kafka/target/`:

* `kafka-<kafka_version>-connector-kinetica-<kinetica_version>.jar` - default JAR
  (not for use)
* `kafka-<kafka_version>-connector-kinetica-<kinetica_version>-tests.jar` - tests
  JAR (see below how to use it to test connectivity)
* `kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar` -
  complete connector JAR

where `<kafka_version>` and `<kinetica_version>` are configured in `pom.xml`
*properties*:

```xml
    <properties>
        <gpudb-api.version>[<kinetica_version>,7.2.0.0-SNAPSHOT)</gpudb-api.version>
        <kafka.version><kafka_version></kafka.version>
    </properties>
```

## Kinetica Kafka Connector Plugin

A Kafka Connect plugin is simply a set of JAR files where Kafka Connect can find
an implementation of one or more connectors, transforms, and/or converters. Kafka
Connect API isolates each plugin from one another so that libraries in one plugin
are not affected by the libraries in any other plugins.

A Kafka Connect plugin is either:

* an *uber JAR* containing all of the classfiles for the plugin and its third-party
dependencies in a single JAR file; see

```bash
kinetica-connector-kafka/kafka-connect-kinetica/target/kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar
```

OR
* a *directory* on the file system that contains the JAR files for the plugin and
its third-party dependencies, see

```bash
kinetica-connector-kafka/kafka-connect-kinetica/target/kafka-<kafka_version>-connector-kinetica-<kinetica_version>-package
|-- etc
    |-- kafka-connect-kinetica
        |-- quickstart-kinetica-sink.properties
        |-- quickstart-kinetica-source.properties
|-- share
    |-- doc
        |-- kafka-connect-kinetica
            |-- licenses
                |-- LICENSE.apache2.0.txt
                |-- LICENSE.bsd.txt
                |-- ...
            |-- LICENSE
            |-- NOTICE
            |-- version.txt
    |-- java
        |-- kafka-connect-kinetica
            |-- ...
            |-- kafka-connect-kinetica-<kinetica_version>.jar
            |-- ...
            |-- xz-1.5.jar
```


### Installing Kinetica Kafka Connector on plain Kafka stack

To install the connector at the target server location for plain **Kafka**, check
the `KAFKA_HOME/config/connect-distributed.properties` and
`KAFKA_HOME/config/connect-standalone.properties` files for the
`plugin.path` property value, then copy the uber JAR
`kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar`
into that location. If the `plugin.path` location has not been set, create a folder
`/KAFKA_HOME/plugins` or `/KAFKA_HOME/connectors`, then copy the uber JAR
`kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar`
into that location and edit `connect-distributed.properties` and
`connect-standalone.properties` files, adding the `plugins.path` property with
the path to Kinetica connector jar location. Make sure other properties
in `connect-distributed.properties` and `connect-standalone.properties` files
match the configuration you've tested with your Kafka connector. If they don't
match, create a new folder `kafka-connect-kinetica` under the `KAFKA_HOME/config/`
location and save your customized property files there, as well as any additional
properties files you might need, such as `kinetica-sink.properties`,
`kinetica-source.properties`.

### Installing Kinetica Kafka Connector on Confluent platform

A quick installation of connector at the target server location for **Confluent**
platform is very similar to installing connector on Kafka stack. Check the property
file `/CONFLUENT_HOME/etc/kafka/connect-standalone.properties` for the `plugin.path`
value, then copy the uber JAR
`kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar`
into that location. If the `plugin.path` location has not been set, create a folder
`/CONFLUENT_HOME/plugins` or `/CONFLUENT_HOME/connectors` then copy the uber JAR
`kafka-<kafka_version>-connector-kinetica-<kinetica_version>-jar-with-dependencies.jar`
into that location and add the `plugin.path` value with this path to
`connect-standalone.properties` and `connect-distributed.properties` file.
Make sure other properties in `connect-distributed.properties` and
`connect-standalone.properties` files match the configuration you've
tested with your Kafka connector. If they don't match, create a new folder
`kafka-connect-kinetica` under the `CONFLUENT_HOME/etc/` location and save your
customized property files there, as well as any additional properties files you
might need, such as `kinetica-sink.properties`, `kinetica-source.properties`.

> **Note:** Separate directory for connector plugins ensures that dependecy
conflicts between Kafka/Confluent and connector do not jeopardise health of the
Kafka stack. Do not drop connector jar directly into `KAFKA_HOME/libs/` or
`/CONFLUENT_HOME/share/java/` folders.

Confluent recommends the following procedure to deploy a well-tested connector
plugin in production environment:

Check the `kafka-connect-kinetica` project `target` folder after the build
completed, it should contain the artifact folder
`kafka-<kafka_version>-connector-kinetica-<kinetica_version>-package`
Follow the same directory structure you find in the build artifact and copy
files into `CONFLUENT_HOME` directories:

```sh
mkdir /CONFLUENT_HOME/share/java/kafka-connect-kinetica
cp target/kafka-<kafka_version>-connector-kinetica-<kinetica_version>-package/share/java/* /CONFLUENT_HOME/share/java/kafka-connect-kinetica/
mkdir /CONFLUENT_HOME/etc/kafka-connect-kinetica
cp target/kafka-<kafka_version>-connector-kinetica-<kinetica_version>-package/etc/* /CONFLUENT_HOME/etc/kafka-connect-kinetica/
mkdir /CONFLUENT_HOME/share/doc/kafka-connect-kinetica
cp target/kafka-<kafka_version>-connector-kinetica-<kinetica_version>-package/share/doc/* /CONFLUENT_HOME/share/doc/kafka-connect-kinetica/
```

> **Note:** Following this convention accurately when naming folders and placing
  connector jar and its configuration properties accordingly is essential to load
  and start/stop the connector remotely through REST service. Unique connector name
  in `quickstart-kinetica-*.properties` would be passed to REST service.
> `kafka-connect-kinetica` folder name would be treated both as the connector
  identification and as a part of the path built on the fly when the connector is
  engaged.
> Starting folder name with `kafka-connect-<connector name>` is a Confluent
  convention used for all Kafka Connect components, such as jdbc, s3, hdfs, and
  others.


## Kinetica Kafka Connector Plugin Deployment

Users can run Kafka Connect in two ways: standalone mode or distributed mode.

In *standalone mode*, a single process runs a single connector listening on the
port provided in worker configuration. It is not fault tolerant, the connector
is not going to be restarted upon failure.
Since it uses only a single process, it is not scalable. Standalone mode is used
for proof of concept and demo purposes, integration or unit testing, and we
recommend it should be managed through the CLI.

In *distributed mode*, multiple workers run Kafka Connectors and are aware of
each others' existence, which can provide fault tolerance and coordination between
them and during the event of reconfiguration. In this mode, Kafka Connect is
scalable and fault tolerant, so it is generally used in production deployment.
Distributed mode provides flexibility, scalability and high availability, it's
mostly used in production in cases of heavy data volume, and it is managed through
REST interface.

Deploying Kinetica-Kafka Connector is covered in detail in [Managing Kafka Connector][MANAGING_KAFKA].

## Streaming Data from Kinetica into Kafka

The `KineticaSourceConnector` can be used as-is by **Kafka Connect** to stream
data from Kinetica into Kafka. The connector will create table monitors to listen
for inserts on a set of tables and publish the inserted rows to separate topics.
A separate **Kafka topic** will be created for each database table configured.
Data will be streamed in flat **Kafka Connect** `Struct`  format
with one field for each table column.

The `KineticaSourceConnector` is configured through `KineticaSourceConnectorConfig`
class using `quickstart-kinetica-source.properties` file that accepts the following
parameters:

| Property Name | Required | Description |
| :--- | :--- | :--- |
| `name` | Y | Name for the connector |
| `connector.class` | Y | Must be `com.kinetica.kafka.KineticaSourceConnector` |
| `tasks.max` | Y | Number of threads |
| `kinetica.url` | Y | The URL of the Kinetica database server |
| `kinetica.username` | N | Username for authentication |
| `kinetica.password` | N | Password for authentication |
| `kinetica.table_names`| Y | A comma-delimited list of names of tables to stream from |
| `kinetica.topic_prefix`| Y | Token prepended to the name of each topic (see below) |
| `kinetica.timeout` | N | Timeout in milliseconds (default = none) |

The connector uses the `kinetica.topic_prefix` to generate the name for destination
topic from the `kinetica.table_names`. For example, if topic_prefix is `Tweets.`
and an insert is made to table `KafkaConnectorTest` then it would  publish the
change to topic `Tweets.KafkaConnectorTest`.


* Edit the configuration file `quickstart-kinetica-source.properties` for the
source connector:

```
name = <UniqueNameOfSourceConnector>
connector.class = com.kinetica.kafka.KineticaSourceConnector
tasks.max = 1
kinetica.url = <KineticaServiceURL>
kinetica.username = <KineticaAuthenticatingUserName>
kinetica.password = <KineticaAuthenticatingUserPassword>
kinetica.table_names = <KineticaSourceTableNameA,KineticaSourceTableNameB>
kinetica.timeout = <KineticaConnectionTimeoutInSeconds>
kinetica.topic_prefix = <TargetKafkaTopicNamesPrefix>
```

## Streaming Data from Kafka into Kinetica

The `KineticaSinkConnector` can be used as-is by **Kafka Connect** to stream data
from Kafka into Kinetica. Streamed data must be in a flat **Kafka Connect**
`Struct` that uses only supported data types for fields (`BYTES`, `FLOAT64`,
`FLOAT32`, `INT32`, `INT64`, and `STRING`). No transformation is performed on
the data and it is streamed directly into a table. The target table and schema
will be created if they don't exist and user has sufficient privileges.

**Warning:** If the target table does not exist, the connector will create it
based on the information available in the Kafka schema. This information is
missing gpudb column attributes like `timestamp`, `shard_key`, and `charN`. If
these attributes are important then you should create the table in advance of
running the connector so it will use the existing table.


The `KineticaSinkConnector` is configured through `KineticaSinkConnectorConfig`
using `quickstart-kinetica-sink.properties` file (available in
`kafka-connect-kinetica/config`) that accepts the following parameters:

| Property Name | Required | Description |
| :--- | :--- | :--- |
| `name` | Y | Name for the connector |
| `connector.class` | Y | Must be `com.kinetica.kafka.KineticaSinkConnector` |
| `topics` | Y | Comma separated list of topics to stream from |
| `topics.regex` | N | Regular expression applied to all available topic names to stream from |
| `tasks.max` | Y | Number of threads |
| `kinetica.url`| Y | The URL of the Kinetica database server (i.e. http://127.0.0.1:9191) |
| `kinetica.username`| N | Username for authentication |
| `kinetica.password`| N | Password for authentication |
| `kinetica.table_prefix`| N | Prefix for destination tables (see below) |
| `kinetica.dest_table_override`| N | Override for table name. (see below) |
| `kinetica.collection_name`| Y | Collection to put the table in (default is empty) |
| `kinetica.batch_size`| N | The number of records to insert at one time (default = 10000) |
| `kinetica.timeout`| N | Timeout in milliseconds (default = 1000) |
| `kinetica.enable_multihead`| N | Automatically enable multihead ingest (default = true) |
| `kinetica.create_table`| N | Automatically create missing table. (default = true) |
| `kinetica.update_on_existing_pk`| N | Allow UPSERT of data into Kinetica table on existing PK. (default = true) |
| `kinetica.allow_schema_evolution`| N | Allow schema evolution support for Kafka messages (requires Schema Registry running in Kafka stack). (default = false) |
| `kinetica.single_table_per_topic`| N | When true, connector attempts to put all incoming messages into a single table. Otherwise creates a table for each individual message type.  (default = false) |
| `kinetica.add_new_fields_as_columns`| N | When schema evolution is supported and Kafka message has a new field, connector attempts to insert a column for it into Kinetica table. (default = false) |
| `kinetica.make_missing_field_nullable`| N | When schema evolution is supported and Kafka message does not have a required field, connector attempts to alter corresponding table column, making it nullable. (default = false) |
| `kinetica.retry_count`| N | Number of attempts to insert data before task fails. (default = 1) |
| `kinetica.flatten_source.enabled`| N | When true, connector attempts to flatten nested schema for incoming messages. (default = false) |
| `kinetica.flatten_source.field_name_delimiter` | N | Symbol to be used when concatenating nested schema field names into Kinetica column name. (default '_') |
| `kinetica.flatten_source.array_flattening_mode` | N | Different modes to convert ARRAY values into single column. (default CONVERT_TO_STRING) |
| `kinetica.flatten_source.array_element_separator` | N | Custom value separator used when stringifying and concatenating stringified Array values into string during schema flattening. (default = ,) |

`topics` and `topics.regex` parameters are mutually exclusive. Either Kafka `topics` names to subscribe to
are provided explicitly, or a regular expression `topics.regex` is applied to all available Kafka topics
until matches are found. Kafka expects `topics.regex` value to start with carrot symbol (`^`).

You can use the optional  `table_prefix` parameter to prepend a token to the table name.
This is useful for testing where you have a source connector reading from multiple tables and
you want the sink connector to write same data objects into different tables of the same database.

You can also use the optional `dest_table_override` parameter to manually specify a table name not
generated from the Kafka schema. When `topics` parameter has a comma-separated list of topic names,
`dest_table_override` should either be a comma-separated list of the same length or be left blank.
When topics are defined by `topics.regex` expression, `dest_table_override` parameter is not applicable.

You can enable schema flattening mode on source schema by setting `kinetica.flatten_source.enabled`
flag to true choosing the actual mode in `kinetica.flatten_source.array_flattening_mode` parameter
(default value CONVERT_TO_STRING). When the flattening is done by conversion to string, the array
values are stringified and concatenated with `kinetica.flatten_source.array_element_separator`
symbol, such as `,` or `|` (default is `,`). All nested fields' names are converted to Kinetica
column name by joining names as `parent_name.child_name`, and the joining symbol can be configured in
`kinetica.flatten_source.field_name_delimiter` parameter.

**Warning:** If `kinetica.single_table_per_topic` is set to false, there would be multiple Kinetica tables
created for a single topic. Connector determines the name of the destination table based on the schema
attached to the message.

**Warning:** If `kinetica.single_table_per_topic` is set to true, a single table with topic
name is created and all the incoming data would be formatted to fit its structure and column types.

**Note** You can find more on schema evolution rules in [Apache Avro specification][AVRO_SCHEMA_EVOLUTION]
and in [Kafka Schema Evolution][KAFKA_SCHEMA_EVOLUTION] site section.

**Warning:** If `kinetica.allow_schema_evolution` is set to `true`, Connector would attempt to lookup
schema versions of the message object through Schema Registry. Additional parameters
`kinetica.add_new_fields_as_columns` and `kinetica.make_missing_field_nullable` allow to
modify Kinetica table on the fly upon receiving Kafka message with new or missing fields.
Default values for `kinetica.allow_schema_evolution`, `kinetica.add_new_fields_as_columns`
and `kinetica.make_missing_field_nullable` are set to `false`, because mapping schemas and
altering tables are expensive time-consuming operations, these options should be used
when absolutely necessary. This set of parameters was added to support Avro Schema Evolution
and connecting to Schema Registry. If `kinetica.allow_schema_evolution` is set to `false`, the
Connector assumes object format is not going to change over time and would not attempt to map
field names and types of incoming data to cached schema even if Schema Registry service is available.
Every schema version other that the version available at the time Connector was subscribed to topic
would be blacklisted and data in that format ignored.

If you intend to use Kafka Schema Registry (with or without a possibility of Schema Evolution), please
configure the following parameters in your `connect-standalone.properties` file. Schema registry service is
usually set up on the same server as bootstrap-server, port 8081.

```
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://localhost:8081
key.converter.schemas.enable=true
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081
value.converter.schemas.enable=true
```

* Edit the configuration file `quickstart-kinetica-sink.properties` for the
sink connector:

```
name = <UniqueNameOfSinkConnector>
connector.class = com.kinetica.kafka.KineticaSinkConnector
tasks.max = <NumberOfKafkaToKineticaWritingProcesses>
topics = <TopicPrefix><SourceTableName>
kinetica.url = <KineticaServiceURL>
kinetica.username = <KineticaAuthenticatingUserName>
kinetica.password = <KineticaAuthenticatingUserPassword>
kinetica.tables.schema_name = <TargetKineticaSchemaName>
kinetica.timeout = <KineticaConnectionTimeoutInSeconds>
```

## Configuring Kafka Connector for Secure Connections

If Kafka stack is configured to allow only secure connections, add the following
parameters to `connect-standalone.properties` or `connect-destributed.properties`:

### Configuring SSL Certificates with Server-Side Truststore

``` bash
ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
ssl.truststore.password=<password>
ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
ssl.keystore.password=<password>
ssl.key.password=<password>
```

Additional parameters are available for secure connection to Schema Registry
server:

``` bash
inter.instance.protocol=http
schema.registry.url: "<schema-registry-url>:<port>"
schema.registry.ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
schema.registry.ssl.truststore.password=<password>
schema.registry.ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
schema.registry.ssl.keystore.password=<password>
schema.registry.ssl.key.password=<password>
```

### Basic HTTP Authentication

If the Kafka stack is configured for basic HTTP Authentication, you may need to
use the following parameters:

``` bash
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<username>:<password>
```

### Authentication with SASL

Kafka uses the Java Authentication and Authorization Service (JAAS) for SASL
configuration. You must provide JAAS configurations for all SASL authentication
mechanisms.

``` bash
sasl.mechanism=PLAIN
# Configure SASL_SSL if SSL encryption is enabled, otherwise configure SASL_PLAINTEXT
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<username>" password="<password>";
```

> **Note:** While setting up `sasl.jaas.config` value, please follow the format
  provided, do not break value into multiple lines, do not replace double quotes
  with single quotes, use the semicolon at the end of the value.

When your Kafka connector configured in the local Kafka/Confluent stack is
connecting to Cloud Kafka, you would need to set additional SASL-related properties
prefixed with `producer.` for source connector and `consumer.` for sink connector.
AWS <cluster API key> is used for <username>, and <cluster API secret> is used
for <password>.

``` bash
bootstrap.servers=<your kafka cloud>.aws.confluent.cloud:9092
ssl.endpoint.identification.algorithm=https
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<cluster API key >" password="<cluster API secret>";

producer.security.protocol=SASL_SSL
producer.sasl.mechanism=PLAIN
producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<cluster API key >" password="<cluster API secret>";

consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<cluster API key >" password="<cluster API secret>";
```

For any other Encryption and Security related Kafka options (such as SASL/GSSAPI,
SASL OAUTHBEARER, SASL/SCRAM, Delegation Tokens, LDAP, and Kerberos), please
visit the [Confluent web site][KAFKA_SECURITY] and follow directions
for setting up Kafka Client configuration.


## Connector Error Handling

Starting with Kafka 2.0.0 (Confluent 5.0.0) you can configure additional
error-handling and error-logging options:

| Property Name | Required | Description | Values |
| :--- | :--- | :--- | :--- |
| `errors.tolerance`| N | whether to fail task upon exceeding retires |  none, all; (default = none) |
| `errors.retry.timeout`| N | total duration of retried for failed operation |  -1, 0, …, Long.MAX_VALUE; (default 0; -1 means infinite retries) |
| `errors.retry.delay.max.ms`| N | delay between two consecutive retries | 1, …, Long.MAX_VALUE; (default 60000) |
| `errors.log.enable`| N | log the error context along with the other application logs. This context includes details about the failed operation, and the record which caused the failure | true/false; (default false) |
| `errors.log.include.messages`| N | whether to include the content of failed ConnectRecord in log message | true/false; (default false) |
| `errors.deadletterqueue.topic.name`| N | Kafka topic name to redirect failed messages | String (default “”) |
| `errors.deadletterqueue.topic.replication.factor`| N | replication factor used to create the dead letter queue topic when it doesn't already exist | 1, …, Short.MAX_VALUE; (default 3) |
| `errors.deadletterqueue.context.headers.enable`| N | if true, multiple headers will be added to annotate the record with the error context | true/false; (default false) |

These options are added to connector configuration files `quickstart-kinetica-sink.properties` and
`quickstart-kinetica-source.properties`.
The `errors.deadletterqueue` family of options are valid for sink connectors only.


## System Test

The following exercise allows you to test your Kafka connector setup. It has
three steps:
- creating and populating Kinetica tables with test data through Datapump;
- running a source connector to send Kinetica table data to Kafka topic;
- running a sink connector to send Kafka topic data to Kinetica tables.

As an alternative, you can run a python KafkaProducer script to generate data
for a Kafka topic, then run a sink connector to populate Kinetica tables, then
run a source connector to create another Kafka topic.

### Datapump Test Utility

The datapump utility is used to generate insert activity on tables to facilitate
testing. It will create tables `KafkaConnectorTest` and `KafkaConnectorTest2`
and insert records at regular intervals.

```sh
usage: TestDataPump [options] [URL]
 -c,--configFile <path>      Relative path to configuration file.
 -d,--delay-time <seconds>   Seconds between batches.
 -h,--help                   Show Usage
 -n,--batch-size <count>     Number of records in a batch.
 -t,--total-batches <count>  Number of batches to insert.
```

The below example (built for kafka 2.6.0 amd kinetica 7.0.0.0) runs the datapump
with default options on a local Kinetica instance (not password-protected) and
will insert batches of 10 records every 3 seconds.

```sh
java -cp kafka-2.6.0-connector-kinetica-7.0.0.0-tests.jar:kafka-2.6.0-connector-kinetica-7.0.0.0-jar-with-dependencies.jar \
    com.kinetica.kafka.TestDataPump http://localhost:9191
```

You can also provide a relative path to Kinetica DB instance configuration file
that contains URL, username, password and timeout:

```sh
java -cp kafka-2.6.0-connector-kinetica-7.0.0.0-tests.jar:kafka-2.6.0-connector-kinetica-7.0.0.0-jar-with-dependencies.jar \
    com.kinetica.kafka.TestDataPump -c config/quickstart-kinetica-sink.properties
```

### Python KafkaProducer code

``` python
from kafka import KafkaProducer
from kafka.errors import KafkaError
from flask import request, json
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Block for 'synchronous' sends
try:
    for i in range(100):
      data = {}
      data['symbol'] = 'AA'+str(i)
      data['sector'] = 'equipment'
      data['securityType'] = 'commonstock'
      data['bidPrice'] = random.randint(0,20)
      data['bidSize'] = random.randint(0,10)
      data['askPrice'] = random.randint(0,20)
      data['askSize'] = random.randint(0,10)
      data['lastUpdated'] = 1547587673240
      data['lastSalePrice'] = 153.04
      data['lastSaleSize'] = 100
      data['lastSaleTime'] = 1547585999856
      data['volume'] = 757810
      data['marketPercent'] = 0.0267
      json_data = json.dumps(data)
      producer.send('NewTestTopic1', json_data.encode(), 'table')

except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass


def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

producer.flush()
```

### Configuring Connectors

This test will demonstrate the *Kinetica Kafka Connector* source and sink in
standalone mode. The standalone mode should be used only for testing. You should
use  [distributed mode][DIST_MODE] for a production deployment.

[DIST_MODE]: <https://docs.confluent.io/current/connect/managing.html#configuring-connectors>

In the `{KAFKA_HOME}/config` folder create configuration files
`connect-standalone-sink.properties` and `connect-standalone-source.properties`
based on example below. Make sure `rest.port` values for sink and source files
are set to different values. This example may require modifications (editing IP
addresses, ports, local paths) to fit your environment.

```
# This should point to your Kafka broker
bootstrap.servers = localhost:9092

offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms = 5000

# These ports must not be used by other processes on the host.
# source rest port
rest.port = 8089
# sink rest port
# rest.port = 8090

# Key is stored in commit log with JSON schema.
key.converter = org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable = true

# Value is stored in commit log with JSON schema.
value.converter = org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable = true

# Disable schemas for internal key/value parameters:
internal.key.converter = org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable = false
internal.value.converter = org.apache.kafka.connect.json.JsonConverter
internal.value.converter.schemas.enable = false
```

In the same folder create a configuration file `source.properties` for the
source connector:

```
# Connector API required config parameters
name = TwitterSourceConnector
connector.class = com.kinetica.kafka.KineticaSourceConnector
tasks.max = 1

# Kinetica specific config parameters
kinetica.url = http://localhost:9191
kinetica.table_names = KafkaConnectorTest,KafkaConnectorTest2
kinetica.timeout = 1000
kinetica.topic_prefix = Tweets.
```

Create a configuration file `sink.properties` for the sink connector:

```
name = TwitterSinkConnector
topics = Tweets.KafkaConnectorTest,Tweets.KafkaConnectorTest2
connector.class = com.kinetica.kafka.KineticaSinkConnector
tasks.max = 1

# Kinetica specific config
kinetica.url = http://localhost:9191
kinetica.collection_name = TEST
kinetica.table_prefix = out_
kinetica.timeout = 1000
kinetica.batch_size = 100
```

### Running the Test

The rest of this system test will require four terminal windows.

* In terminal 1, start *zookeeper* and *kafka*:

```sh
$ cd <path/to/Kafka>
$ bin/zookeeper-server-start.sh config/zookeeper.properties &
$ bin/kafka-server-start.sh config/server.properties
```

* In terminal 2, start test datapump. This will create the `KafkaConnectorTest`
and `KafkaConnectorTest2` tables and generate insert activity.

```sh
$ java -cp kafka-2.6.0-connector-kinetica-7.0.0.0-tests.jar:kafka-2.6.0-connector-kinetica-7.0.0.0-jar-with-dependencies.jar \
    com.kinetica.kafka.tests.TestDataPump -c <path/to/sink.properties>
```

* In terminal 3, start kafka sink connector:

```sh
$ bin/connect-standalone.sh config/connect-standalone-sink.properties config/sink.properties
```

* In terminal 4, start kafka source connector:

```sh
$ bin/connect-standalone.sh config/connect-standalone-source.properties config/source.properties
```

* Verify that data is copied to tables `out_KafkaConnectorTest` and `out_KafkaConnectorTest2`.


To test schemaless JSON format, in `connect-standalone-*.properties` config files set

```
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

For more information, please refer to configuration descriptions and test scenarios
in [test-connect/README.md][TEST_LOC]
[TEST_LOC]: <test-connect/README.md>
