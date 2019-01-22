# Kinetica Kafka Connector

The following guide provides step by step instructions how to build and integrate 
Kinetica connector with Kafka platform. It can be integrated as a part of Kafka stack or 
Confluent platform.

This project is aimed to make Kafka topics accessible to Kinetica, meaning data can be streamed 
from a Kinetica table or to a Kinetica table via **Kafka Connect**. The custom **Kafka Source Connector**
and **Kafka Sink Connector** do no additional processing. (There is a separate set of parameters in 
connect-standalone.properties that allows lightweight message-at-a-time data transformations, such as 
create a new column from Kafka message timestamp. It’s a part of worker task configuration that’s configured 
as a part of Kafka Connect.)

The two connector classes that integrate **Kinetica** with **Kafka** are:
* `com.kinetica.kafka.KineticaSourceConnector` - A *Kafka Source Connector*, which receives a data
  stream from the database via table monitor
* `com.kinetica.kafka.KineticaSinkConnector` - A *Kafka Sink Connector*, which receives a data
  stream from a *Kafka Source Connector* and writes it to the database

## Resources

* [Connector GitHub repository][REPOSITORY]
* [Kafka Developer Manual][MANUAL]
* [Confluent Kafka Connect][KAFKA]

[REPOSITORY]: <https://github.com/kineticadb/kinetica-connector-kafka>
[MANUAL]: <https://www.kinetica.com/docs/connectors/kafka_guide.html>
[KAFKA]: <https://docs.confluent.io/current/connect/index.html>
[AVRO_SCHEMA_EVOLUTION]: <https://avro.apache.org/docs/1.8.1/spec.html>
[KAFKA_SCHEMA_EVOLUTION]: <https://docs.confluent.io/current/schema-registry/docs/avro.html>

## Installation & Configuration
The connector provided in this project assumes launching will be done on a server capable of
running Kinetica Kafka connectors in standalone mode or to a cluster. 

### Version support

Kinetica Kafka connector has a property parameter in the `pom.xml` properties to set **Kafka** version. 
Connector build process would add **Kafka** version to the jar name for easy reference: 
`kafka-2.0.0-connector-kinetica-6.2.0-jar-with-dependencies.jar`. 
Connector code is Java 7 compatible and does not require a separate build to support Java 8 environment. 

**Kafka Connect** allows you to configure the Kinetica Kafka Connector exactly the same for 
plain **Kafka** stack or integrated **Confluent** platform. The following table is provided 
for the ease of identifying compatible dependency versions. Based on Kafka version or Confluent platform 
version, edit `pom.xml` project properties to build connector compatible with your **Kafka** version. 

| Confluent Platform | Apache Kafka | Java version | 
| :--- | :--- | :--- | 
| 3.1.x | 0.10.1.x | 1.7.0_60, 1.8.0_60 |
| 3.2.x | 0.10.2.x | 1.7.0_60, 1.8.0_60 |
| 3.3.x | 0.11.0.x | 1.7.0_60, 1.8.0_60 |
| 4.0.x | 1.0.x | 1.7.0_60, 1.8.0_60 |
| 4.1.x | 1.1.x | 1.7.0_60, 1.8.0_60 |
| 5.0.x | 2.0.x | 1.8.0_60 |
| 5.1.x | 2.1.x | 1.8.0_60 |


Clone and build the project as follows: 

```sh
git clone https://github.com/gisfederal/kinetica-connector-kafka
cd kinetica-connector-kafka/kafka-connect-kinetica
mvn clean compile package -DskipTests=true
```

To run JUnit tests as part of build process, make sure that you have a running Kinetica instance, and that 
its URL and user credentials are properly configured in files `config/quickstart-kinetica-sink.properties`
and `config/quickstart-kinetica-source.properties`, then run

```sh
mvn clean compile package
```

Three JAR files are produced by the Maven build in `kinetica-connector-kafka/target/`:

* `kafka-<ver>-connector-kinetica-<ver>.jar` - default JAR (not for use)
* `kafka-<ver>-connector-kinetica-<ver>-tests.jar` - tests JAR (see below how to use it to test connectivity)
* `kafka-<ver>-connector-kinetica-<ver>-jar-with-dependencies.jar` - complete connector JAR

### Installing Kinetica Kafka Connector on plain Kafka stack

To install the connector at the target server location for plain **Kafka**, copy the jar 
`kafka-<ver>-connector-kinetica-<ver>-jar-with-dependencies.jar` into `KAFKA_HOME/libs/` folder and edit 
`KAFKA_HOME/config/connect-standalone.properties` file. Any additional properties files you might
need should go in the same folder `KAFKA_HOME/config/`.

### Installing Kinetica Kafka Connector on Confluent platform

To install the connector at the target server location for **Confluent** platform, check the project
target folder it should contain the artifact folder `kafka-<ver>-connector-kinetica-<ver>-package`
Follow the same directory structure you find in the build artifact and copy files into CONFLUENT_HOME
directories:

```sh
mkdir /CONFLUENT_HOME/share/java/kafka-connect-kinetica
cp target/kafka-<ver>-connector-kinetica-<ver>-package/share/java/* /CONFLUENT_HOME/share/java/kafka-connect-kinetica/
mkdir /CONFLUENT_HOME/etc/kafka-connect-kinetica
cp target/kafka-<ver>-connector-kinetica-<ver>-package/etc/* /CONFLUENT_HOME/etc/kafka-connect-kinetica/
mkdir /CONFLUENT_HOME/share/doc/kafka-connect-kinetica
cp target/kafka-<ver>-connector-kinetica-<ver>-package/share/doc/* /CONFLUENT_HOME/share/doc/kafka-connect-kinetica/
```

> **Note** Following this convention accurately when naming folders and placing connector jar and its configuration properties accordingly is essential to load and start/stop the connector remotely through REST service. Unique Connector name in quickstart properties would be passed to REST service.
> `kafka-connect-kinetica` folder name would be treated both as the connector identification and as a part of the path built on the fly when the connector is engaged. 
> Starting folder name with `kafka-connect-<connector name>` is a Confluent convention used for all Kafka Connect components, such as jdbc, s3, hdfs, and others.


### Configuration files location and purpose

When testing the connector in standalone mode, use the following syntax:

```sh
KAFKA_HOME>./bin/connect-standalone.sh config/connect-standalone.properties \
	config/quickstart-kinetica.properties
```
or

```sh
CONFLUENT_HOME>bin/connect-standalone etc/kafka/connect-standalone.properties \
	etc/kafka-connect-kinetica/quickstart-kinetica.properties
```
Where `connect-standalone.properties` is used to configure worker tasks and 
`quickstart-kinetica.properties` is used to configure the connector itself. 

Worker tasks and connector configurations use different configuration parameters,
do not add connector configuration params to work task configuration, because they 
would be ignored. Configurations for sink and source connectors differ in the name and 
number of parameters, although some parameters are common.

When you start a connector (sink or source), it should have its own dedicated port 
set with `rest.port` parameter. You can't use the same `connect-standalone.properties` 
file for different connectors running simultaneously, and it is not required, but overall 
very useful to have separate configuration files named `connect-standalone-sink.properties` 
and `connect-standalone-source.properties` with preset port values, such as sink `rest.port=8090` 
and source `rest.port=8089`. 

For remote REST invocation of sink or source connector please refer to [Confluent web site][CONFLUENT_REST_API]. 
[CONFLUENT_REST_API]: <https://docs.confluent.io/current/connect/references/restapi.html>

## Streaming Data from Kinetica into Kafka

The `KineticaSourceConnector` can be used as-is by **Kafka Connect** to stream data from Kinetica
into Kafka. The connector will create table monitors to listen for inserts on a set of
tables and publish the inserted rows to separate topics. A separate **Kafka topic** will be created
for each database table configured. Data will be streamed in flat **Kafka Connect** `Struct`  format
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

The connector uses the `kinetica.topic_prefix` to generate the name for destination topic from the 
`kinetica.table_names`. For example, if topic_prefix is `Tweets.` and an insert is made to table 
`KafkaConnectorTest` then it would  publish the change to topic `Tweets.KafkaConnectorTest`.


* Edit the configuration file `quickstart-kinetica-source.properties` for the source connector:

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

The `KineticaSinkConnector` can be used as-is by **Kafka Connect** to stream data from Kafka into
Kinetica. Streamed data must be in a flat **Kafka Connect** `Struct` that uses only supported data
types for fields (`BYTES`, `FLOAT64`, `FLOAT32`, `INT32`, `INT64`, and `STRING`). No transformation
is performed on the data and it is streamed directly into a table. The target table and collection
will be created if they do not exist.

**Warning:** If the target table does not exist, the connector will create it based on the information
available in the Kafka schema. This information is missing gpudb column attributes like
`timestamp`, `shard_key`, and `charN`. If these attributes are important then you should create the
table in advance of running the connector so it will use the existing table. 


The `KineticaSinkConnector` is configured through `KineticaSinkConnectorConfig` 
using `quickstart-kinetica-sink.properties` file (available in kafka-connect-kinetica/config) 
that accepts the following parameters:

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
| `kinetica.create_table`| N | Automatically create missing table. (default = true) |
| `kinetica.allow_schema_evolution`| N | Allow schema evolution support for Kafka messages (requires Schema Registry running in Kafka stack). (default = false) |
| `kinetica.single_table_per_topic`| N | When true, connector attempts to put all incoming messages into a single table. Otherwise creates a table for each individual message type.  (default = false) |
| `kinetica.add_new_fields_as_columns`| N | When schema evolution is supported and Kafka message has a new field, connector attempts to insert a column for it into Kinetica table. (default = false) |
| `kinetica.make_missing_field_nullable`| N | When schema evolution is supported and Kafka message does not have a required field, connector attempts to alter corresponding table column, making it nullable. (default = false) |
| `kinetica.retry_count`| N | Number of attempts to insert data before task fails. (default = 1) |

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

* Edit the configuration file `quickstart-kinetica-sink.properties` for the sink connector:

```
name = <UniqueNameOfSinkConnector>
connector.class = com.kinetica.kafka.KineticaSinkConnector
tasks.max = <NumberOfKafkaToKineticaWritingProcesses>
topics = <TopicPrefix><SourceTableName>
kinetica.url = <KineticaServiceURL>
kinetica.username = <KineticaAuthenticatingUserName>
kinetica.password = <KineticaAuthenticatingUserPassword>
kinetica.collection_name = <TargetKineticaCollectionName>
kinetica.timeout = <KineticaConnectionTimeoutInSeconds>
```

### Additional error-handling options

Starting with Kafka 2.0.0 (Confluent 5.0.0) you can configure additional error-handling
and error-logging options:

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


### Datapump Test Utility

The datapump utility is used to generate insert activity on tables to facilitate testing. It will
create tables `KafkaConnectorTest` and `KafkaConnectorTest2` and insert records at regular intervals.

```sh
usage: TestDataPump [options] [URL]
 -c,--configFile <path>      Relative path to configuration file.
 -d,--delay-time <seconds>   Seconds between batches.
 -h,--help                   Show Usage
 -n,--batch-size <count>     Number of records in a batch.
 -t,--total-batches <count>  Number of batches to insert.
```

The below example (built for kafka 2.0.0 amd kinetica 6.2.1) runs the datapump with default options on a local 
Kinetica instance (not password-protected) and will insert batches of 10 records every 3 seconds.

```sh
java -cp kafka-2.0.0-connector-kinetica-6.2.1-tests.jar:kafka-2.0.0-connector-kinetica-6.2.1-jar-with-dependencies.jar \
    com.kinetica.kafka.TestDataPump http://localhost:9191
```

You can also provide a relative path to Kinetica DB instance configuration file that contains URL, username, password and timeout:

```sh
java -cp kafka-2.0.0-connector-kinetica-6.2.1-tests.jar:kafka-2.0.0-connector-kinetica-6.2.1-jar-with-dependencies.jar \
    com.kinetica.kafka.TestDataPump -c config/quickstart-kinetica-sink.properties
```


## System Test

This test will demonstrate the *Kinetica Kafka Connector* source and sink in standalone mode. The
standalone mode should be used only for testing. You should use  [distributed mode][DIST_MODE] for a
production deployment.

[DIST_MODE]: <https://docs.confluent.io/current/connect/managing.html#configuring-connectors>

Create configuration files `connect-standalone-sink.properties` and `connect-standalone-source.properties`
based on example below. Make sure rest.port for sink and source files is set to different values.
This example may require modifications (editing IP addresses, ports, local paths) to fit your environment.

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

Create a configuration file `source.properties` for the source connector:

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

The rest of this system test will require three terminal windows.

* In terminal 1, start *zookeeper* and *kafka*:

```sh
$ cd <path/to/Kafka>
$ bin/zookeeper-server-start.sh config/zookeeper.properties &
$ bin/kafka-server-start.sh config/server.properties
```

* In terminal 2, start test datapump. This will create the `KafkaConnectorTest` and `KafkaConnectorTest2`
tables and generate insert activity.

```sh
$ java -cp kafka-2.0.0-connector-kinetica-6.2.0-jar-with-dependencies.jar \
    com.kinetica.kafka.tests.TestDataPump -c <path/to/sink.properties>
```

* In terminal 3, start kafka source and sink connectors:

```sh
$ connect-standalone connect-standalone.properties \
    source.properties sink.properties
```

* Verify that data is copied to tables `out_KafkaConnectorTest` and `out_KafkaConnectorTest2`.


To test schemaless JSON format, in `connect-standalone.properties` file set 

```
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

For more information, please refer to configuration descriptions and test scenarios in [test-connect/README.md][TEST_LOC]
[TEST_LOC]: <test-connect/README.md>
