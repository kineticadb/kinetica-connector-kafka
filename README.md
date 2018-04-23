# Kafka Developer Manual

The following guide provides step by step instructions to get started integrating Kinetica with
Kafka.

This project is aimed to make Kafka topics accessible to Kinetica, meaning data can be streamed from a
Kinetica table or to a Kinetica table via **Kafka Connect**.  The custom **Kafka Source Connector**
and **Sink Connector** do no additional processing.

The two connector classes that integrate **Kinetica** with **Kafka** are:
* `com.kinetica.kafka.KineticaSourceConnector` - A *Kafka Source Connector*, which receives a data
  stream from the database via table monitor
* `com.kinetica.kafka.KineticaSinkConnector` - A *Kafka Sink Connector*, which receives a data
  stream from a *Kafka Source Connector* and writes it to the database

## Resources

* [Connector GitHub repository][REPOSITORY]
* [Kafka Developer Manual][MANUAL]
* [Confluent Kafka Connect][KAFKA]
* [Docker test environment for Kafka Connecter][DOCKER]

[REPOSITORY]: <https://github.com/kineticadb/kinetica-connector-kafka>
[MANUAL]: <https://www.kinetica.com/docs/connectors/kafka_guide.html>
[KAFKA]: <https://www.kinetica.com/docs/connectors/kafka_guide.html>
[DOCKER]: <https://bitbucket.org/gisfederal/gpudb-docker/src/HEAD/kafka-connect/?at=master>

## Streaming Data from Kinetica into Kafka

The `KineticaSourceConnector` can be used as-is by **Kafka Connect** to stream data from Kinetica
into Kafka. The connector will create table monitors to listen for inserts or updates on a set of
tables and publish the  updated rows to separate topics. A separate **Kafka topic** will be created
for each database table configured. Data will be streamed in flat **Kafka Connect** `Struct`  format
with one field for each table column.

The `KineticaSourceConnector` is configured using a properties file that accepts the following
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

The connector uses the `topic_prefix` to generate the name of a destination topic from the table
name. For example if topic_prefix is `Tweets.` and an insert is made to table `KafkaConnectorTest`
then it would  publish the change to topic `Tweets.KafkaConnectorTest`.

## Streaming Data from Kafka into Kinetica

The `KineticaSinkConnector` can be used as-is by *Kafka Connect* to stream data from Kafka into
Kinetica. Streamed data must be in a flat *Kafka Connect* `Struct` that uses only supported data
types for fields (`BYTES`, `FLOAT64`, `FLOAT32`, `INT32`, `INT64`, and `STRING`). No transformation
is performed on the data and it is streamed directly into a table. The target table and collection
will be created if they do not exist.

The `KineticaSinkConnector` is configured using a properties file that accepts the following
parameters:

| Property Name | Required | Description |
| :--- | :--- | :--- |
| `name` | Y | Name for the connector |
| `connector.class` | Y | Must be `com.kinetica.kafka.KineticaSinkConnector` |
| `topics`| Y | Comma separated list of topics to stream from |
| `tasks.max` | Y | Number of threads |
| `kinetica.url`| Y | The URL of the Kinetica database server |
| `kinetica.username`| N | Username for authentication |
| `kinetica.password`| N | Password for authentication |
| `kinetica.table_prefix`| N | Prefix for destination tables (see below) |
| `kinetica.dest_table_override`| N | Override for table name. (see below) |
| `kinetica.collection_name`| Y | Collection to put the table in (default is empty) |
| `kinetica.batch_size`| N | The number of records to insert at one time (default = 10000) |
| `kinetica.timeout`| N | Timeout in milliseconds (default = 1000) |
| `kinetica.create_table`| N | Automatically create missing table. (default = true) |

The connector determines the name of the destination table based on the Avro schema attached to the
message.

You can use the optional  `table_prefix` parameter to have it prepend a token to the table
name. This is useful for testing where you have a source connector reading from multiple tables and
you want the sink connector to write to different tables in the same database.

You can also use the optional `dest_table_override` parameter to manually specify a table name not
generated from the Kafka schema.

**Note:** This connector does not permit sinking from schema-less records in the Kafka commit log so you
must add the following lines to the `connect-standalone.properties` before running `connect-standalone`:

```
key.converter = org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable = true
value.converter = org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable = true
```

**Warning:** If the target table does not exist the connector will create it based on the information
available in the Kafka schema. This information is missing gpudb column attributes like
`timestamp`, `shard_key`, and `charN`. If these attributes are important then you should create the
table in advance of running the connector so it will use the exiting table.

## Installation & Configuration

The connector provided in this project assumes launching will be done on a server capable of
submitting Kafka connectors in standalone mode or to a cluster.

Two JAR files are produced by this project:

* `kafka-connector-<ver>.jar` - default JAR (not for use)
* `kafka-connector-<ver>-jar-with-dependencies.jar` - complete connector JAR

To install the connector:

* Copy the `kafka-connector-<ver>-jar-with-dependencies.jar` library to the target server
* Create a configuration file `connect-standalone.properties` if you are using standalone mode.
* Create a configuration file `source.properties` for the source connector:
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
* Create a configuration file `sink.properties` for the sink connector:
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

## Datapump Test Utility

The datapump utility is used to generate insert activity on tables to facilitate testing. It will
create tables `KafkaConnectorTest` and `KafkaConnectorTest2` and insert records at regular intervals.

```
usage: TestDataPump [options] URL
 -d,--delay-time <seconds>   Seconds between batches.
 -h,--help                   Show Usage
 -n,--batch-size <count>     Number of records
```

The below example runs the datapump with default options and will insert batches of 10 records every
3 seconds.
```
java -cp kafka-connector-6.2.0-jar-with-dependencies.jar \
    com.kinetica.kafka.tests.TestDataPump \
    http://gpudb:9191
```

## System Test

This test will demonstrate the *Kinetica Kafka Connector* source and sink in standalone mode. The
standalone mode should be used only for testing. You should use  [distributed mode][DIST_MODE] for a
production deployment.

**Note:** As an alternative a pre-configured [Docker stack][DOCKER] is available for testing the connector.

[DIST_MODE]: <https://docs.confluent.io/current/connect/managing.html#configuring-connectors>

Create a configuration file `connect-standalone.properties`. Below is an example and may require modifications.

```
# This should point to your Kafka broker
bootstrap.servers = broker:9092

offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms = 5000

# This port must not be used by another process on the host.
rest.port = 8083

# Make sure your connector jar is in this path.
plugin.path = /opt/connect-test

# Key is stored in commit log with JSON schema.
key.converter = org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable = true

# Value is stored in commit log with JSON schema.
value.converter = org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable = true

# Leave internal key parameters alone
internal.key.converter = org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable = false
internal.value.converter = org.apache.kafka.connect.json.JsonConverter
internal.value.converter.schemas.enable = false
```

Create a configuration file `source.properties` for the source connector:

```
# Connector API required config
name = TwitterSourceConnector
connector.class = com.kinetica.kafka.KineticaSourceConnector
tasks.max = 1

# Kinetic specific config
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

# Kinetic specific config
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
$ java -cp kafka-connector-6.2.0-jar-with-dependencies.jar \
    com.kinetica.kafka.tests.TestDataPump <Kinetica url>
```

* In terminal 3, start kafka source and sink connectors:

```sh
$ connect-standalone connect-standalone.properties \
    source.properties sink.properties
```

* Verify that data is copied to tables `out_KafkaConnectorTest` and `out_KafkaConnectorTest2`.
