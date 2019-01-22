# Kinetica Kafka Connector Additional JUnit and Integration Tests

The following JUnit tests depend on a running Kinetica DB instance, configured in 
`config/quickstart-kinetica-sink.properties`. In the java code directory you can find 
helper classes that can be used as stand-alone applications to access Kafka stack and 
post messages to Kafka topics in various formats, including plain String, 
schemaless JSON, schemaless Oracle Golden Gate JSON, avro encoded JSON, 
and producing messages with multiple schema versions of the same object 
(to test Kafka Connector's implementation of schema evolution). All of these classes
have a required URL configuration option that should point to Kafka bootstrap-server, 
some classes have an additional configuration option for schema registry URL.
These options are required only to access Kafka stack in stand-alone configuration,
provided JUnit tests use data mock-up fully compatible with the data formats
in stand-alone mode.


## Maven build test goal Prerequisites

Current setup of `test-connect` project uses Kinetica addresses: localhost:9191, localhost:8080/gadmin.
Kafka Kinetica Connector uses ports 8090 and 8089 (as Sink and Source ports), if the 
port is undefined, it uses 8083 by default. When starting Kinetica and Kafka on the same machine,
make sure there is no port conflict (some configurations use ports 8088 and 8082 in default
settings which can lead to Confluent rest and ksql services failing).  

Start Kinetica DB and GAdmin (please refer to [Kinetica Documentation for detailed instructions][KINETICA_DOCS_START]) 

Make sure that you have a running Kinetica instance, and that its URL and user credentials 
are properly configured in files `config/quickstart-kinetica-sink.properties`
and `config/quickstart-kinetica-source.properties`.

[KINETICA_DOCS_START]: <https://www.kinetica.com/docs/gpudbAdmin/services.html>

You may need to stop and restart the Connector for different Connector configurations. There is
no need to stop and restart Kinetica DB.


Start maven build with test goal as follows:

```sh
cd kinetica-connector-kafka/test-connect
mvn test
```

At the end of running maven build, you'll see a tally report:

```
Results :

Tests run: 21, Failures: 0, Errors: 0, Skipped: 0
```

If any of the tests failed, please review the Surefire reports available in 
`test-connect/target/surefire-reports` folder.

In case you plan to use the build package later to start separate DataPumps as stand-alone applications,
you can build it as follows:

```sh
cd kinetica-connector-kafka/test-connect
mvn clean compile package 
```

Two JAR files are produced by the Maven build in `kinetica-connector-kafka/target/`:

* `test-connector-<ver>.jar` - default JAR (not for use)
* `test-connector-jar-with-dependencies.jar` - complete connector JAR


## Integration tests prerequisites

[KAFKA_DOC_START]: <https://kafka.apache.org/quickstart>
[CONFLUENT_DOC_START]: <https://docs.confluent.io/current/quickstart/ce-quickstart.html>

Start Kinetica DB and GAdmin (please refer to [Kinetica Documentation for detailed instructions][KINETICA_DOCS_START]). 
Start Confluent (all services at once) or start Kafka Zookeeper, Kafka Server, Kafka Schema Registry 
individually, each in it's own terminal tab. See [Kafka][KAFKA_DOC_START] or 
[Confluent documentation][CONFLUENT_DOC_START] for details.

Not all integration tests would require a Schema Registry. Kafka does not have a Schema Registry in its stack, but it 
can be built as a stand-alone service and deployed as an addition to Kafka stack. 

You can run any of the DataPump classes as a standalone application, option *-h* provides format:

```sh
cd kinetica-connector-kafka/test-connect/target
java -cp test-connector-jar-with-dependencies.jar com.kinetica.kafka.JsonDataPump -h

usage: JsonDataPump [options] URL
 -c,--total-batches <count>        Number of batches to insert.
 -h,--help                         Show Usage
 -n,--batch-size <count>           Number of records in a batch.
 -s,--schema-registry <url:port>   Schema registry location
 -t,--topic <name>                 Topic name

```

DataPump classes available are:

```
com.kinetica.kafka.StringDataPump
com.kinetica.kafka.JsonDataPump
com.kinetica.kafka.AvroSerializerDataPump
com.kinetica.kafka.SchemaTestSerializerDataPump
```

## Simple plain JSON message example - use com.kinetica.kafka.JsonDataPump

In this scenario Kafka topic is sent schemaless JSON records of the same type (Apple).

Connector config parameters (stored in `quickstart-kinetica-sink.properties`) for this configuration:

```
topics = Apple
kinetica.create_table = true
kinetica.collection_name = TEST
kinetica.single_table_per_topic = true
```

`topics` can contain a single topic name or a comma-separated list of topic names. In this test case
it's a single topic getting messages of the same type (Apple).`kinetica.collection_name` is set
to `TEST`. If you provide `kinetica.table_prefix`, it would be prepended to type name when creating 
Kinetica table. 

Now let's take a look at worker config parameters (stored in `connect-standalone-sink.properties`) for this configuration.
This is an example of schemaless JSON, therefore `schemas.enable = false`. It means that provided 
data does not have a Kafka schema provided. 

```
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# internal schemas are disabled as well:
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false
```

Start the process of populating Kafka topic with plain JSON messages:

```sh
java -cp test-connector-jar-with-dependencies.jar com.kinetica.kafka.StringDataPump \
   -n 10 -c 5 -t Apple http://localhost:9092
```

This DataPump would send messages to Kafka topic *Apple* in 5 batches generating 10 records of 
Apple object in each batch and printing out the generated message (sample output):

```
Batch 1
{"id":0,"sort":"AyoRGBqRYc","size":11,"weight":95.27625}
ProducerRecord(topic=Apple, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value={"id":0,"sort":"AyoRGBqRYc","size":11,"weight":95.27625}, timestamp=null)
{"id":1,"sort":"PwHkeOzIrP","size":37,"weight":97.80116}
ProducerRecord(topic=Apple, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value={"id":1,"sort":"PwHkeOzIrP","size":37,"weight":97.80116}, timestamp=null)
{"id":2,"sort":"oCDVSRlhnh","size":43,"weight":87.71193}
ProducerRecord(topic=Apple, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value={"id":2,"sort":"oCDVSRlhnh","size":43,"weight":87.71193}, timestamp=null)
```

In a separate tab start the connector (Kafka and Confluent syntax provided)

Start the sink connector with:

```sh
KAFKA_HOME$> ./bin/connect-standalone.sh config/connect-standalone-sink.properties \
     config/kafka-connect-kinetica/quickstart-kinetica-sink.properties
```
or 

```sh
CONFLUENT_HOME$> bin/connect-standalone etc/kafka/connect-standalone-sink.properties \
     etc/kafka-connect-kinetica/quickstart-kinetica-sink.properties
```

Verify that on Kinetica DB side that Apple table is created and populated with 50 records.

JUnit test com.kinetica.kafka.StringDataPumpSinkTest provides 6 different combinations of 
configuration options for Kafka Connector, including supplying Kinetica table with prefix 
in `kinetica.table_prefix` (results in table outApple), overwriting 
table names in `kinetica.dest_table_override`, and using other config parameters.

Provided JUnit tests send Kafka messages either in schemaless OGG (Oracle Golden Gate) format, or in 
plain schemaless JSON format. OGG test sends table name in message key, so Kafka Connector 
that identifies OGG-specific fields in the message would treat non-null String 
message key as Kinetica table name to be populated with the message value. A null value 
would require Connector to look up table name in `kinetica.table_override` parameter 
in the Connector config or use the topic name as Kinetica table name. 


## DB Table replication (Schemaless JSON example) - use com.kinetica.kafka.JsonDataPump

In this scenario a single Kafka topic is sent new records from multiple DB tables, each 
one has its own table data type (Apple, Orange, Banana). Kafka Sink connector is configured 
to ingest data from this topic, create multiple tables (one for each table data type), and 
insert Kafka records into each table according to record data type. 

Config parameters (stored in `quickstart-kinetica-sink.properties`) for this configuration:

```
topics = fruits
kinetica.create_table = true
kinetica.single_table_per_topic = false
```

`topics` can contain a single topic name or a comma-separated list of topic names.
`kinetica.collection_name` can be provided. If you provide `kinetica.table_prefix`, it would be
prepended to type name. Do not use param `kinetica.dest_table_override` as it is ignored for 
any configuration with `kinetica.single_table_per_topic = false`.

Worker config parameters (stored in `connect-standalone-sink.properties`) for this configuration.

```
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# internal schemas are disabled as well:
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false
```

This is an example of schemaless JSON, therefore `schemas.enable = false`. It means that provided 
data does not have a Kafka schema provided. The table name is stored in key field, as a serialized string. 
Value is a map of name/value pairs and is going to be serialized JSON.

Start the process of populating Kafka topic with plain JSON messages:

```sh
java -cp test-connector-jar-with-dependencies.jar com.kinetica.kafka.JsonDataPump \
   -n 10 -c 5 -t fruits http://localhost:9092
```

This would send messages to Kafka topic *fruits* in 5 batches generating 10 records for each fruit
(Apple, Orange, Banana) and printing out the generated message (sample output):

```
Apple ProducerRecord(topic=TestTopic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=Apple, value={"id":0,"sort":"ENehZTiaKL","size":86,"weight":46.847908}, timestamp=null)
Orange ProducerRecord(topic=TestTopic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=Orange, value={"id":0,"sort":"bZtCZIFnUD","diameter":8,"weight":92.56922}, timestamp=null)
Banana ProducerRecord(topic=TestTopic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=Banana, value={"id":0,"sort":"QaHtyhvYbM","length":4.1626673,"weight":48.574265}, timestamp=null)
```

In a separate tab start the connector (Kafka and Confluent syntax provided)

Start the sink connector with:

```sh
KAFKA_HOME$> ./bin/connect-standalone.sh config/connect-standalone-sink.properties config/kafka-connect-kinetica/quickstart-kinetica-sink.properties
```
or 

```sh
CONFLUENT_HOME$> bin/connect-standalone etc/kafka/connect-standalone-sink.properties etc/kafka-connect-kinetica/quickstart-kinetica-sink.properties
```

Verify that on Kinetica DB side there are 3 tables created (Apple, Banana, Orange) with 50 records each.

A set of JUnit tests com.kinetica.kafka.StringDataPumpSinkTest provides 6 separate combinations of 
configuration options for Kafka Connector, including supplying Kinetica table with prefix 
in `kinetica.table_prefix` (results in tables outApple, outBanana, outOrange), overwriting 
table names in `kinetica.dest_table_override`, etc.



## Schema Evolution support example - use AvroSerializerDataPump and SchemaTestSerializerDataPump

In case you are using Confluent platform and want to check the Kafka Connector working with Schema Registry,
there are two DataPump classes wotking with Avro serialized messages. Schema Registry is running by default
as one of the services when you start Confluent platform, or it can be built and started as an independent 
service for the Kafka stack. 

SchemaTestSerializerDataPump producies servialized Avro messages of given schema version on demand. 
One of the input parameters is Schema Registry address, and another is schema version for SchemaTest object. 
To mimick schema evolution process, there are four pre-existing versions of the same SchemaTest object, 
differing in number and type of parameters. If the provided schema version can't be found, a serialized
Avro schema versionless object of a different type (Employee) is produced.  
To verify that Kafka Connector supports schema evolution, DataPump classes can generate messages in line 
with different kinds of topic  schema evolution compatibility (Forward, Backward or Both). When running
SchemaTestSerializerDataPumpSinkTest JUnit tests, each test is following a specific versioning direction: 
from schema version 1 to schema version4, from schema version 2 to schema version 1, etc. Junit tests 
are not using the actual Schema Registry service, but a mock up KafkaSchemaHelpers tool, while running the 
SchemaTestSerializerDataPump requires actual schema registry service up and running.

Edit connector-config.properties:

```
topics = SchemaTest
kinetica.create_table = true
kinetica.allow_schema_evolution = true
kinetica.single_table_per_topic = true
kinetica.add_new_fields_as_columns = true
kinetica.make_missing_field_nullable = true
```

Worker config parameters for connectors using schema registry (stored in `connect-standalone-sink.properties`) 
have `schemas.enable` set to true, additional parameter for schema registry location and a 
new class for Converter:

```
key.converter.schemas.enable=false
value.converter.schemas.enable=false
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://localhost:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081

# internal schemas are enabled:
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=true
internal.value.converter.schemas.enable=true
```


Start SchemaTestSerializerDataPump as follows:

```sh
cd target
java -cp test-connector-jar-with-dependencies.jar com.kinetica.kafka.SchemaTestSerializerDataPump \
     -t SchemaTest -c 50 -v 1 -s http://localhost:8081 http://localhost:9092
```

SchemaTestSerializerDataPump would produce 50 messages for topic SchemaTest with schema version 1 
using bootstrap server address http://localhost:9092 and schema registry address http://localhost:8981. 
Run the SchemaTestSerializerDataPump for schema version 2 as well, it would produce additional 50 messages.

In a separate tab start the connector (Confluent syntax provided). Start the sink connector with:

```sh
CONFLUENT_HOME$> bin/connect-standalone etc/kafka/connect-standalone-sink.properties \
     etc/kafka-connect-kinetica/quickstart-kinetica-sink.properties
```

When inspecting Kinetica tables, you should see table SchemaTest with 100 records. Take note that first 
50 records in Kinetica table have null values for the last 2 columns (test_int2 and test_long2), and last 50 
records have autogenerated values for these columns. Junit tests for SchemaTestSerializerDataPump run 
separate scenarios with different combinations of Connector config parameters.


The AvroSerializerDataPump has a preset scenario of producing objects of all four schema versions 
in consecutive fashion, schema version 1 through 4. After creating the initial topic, it overwrites
the default Kafka Topic compatibility to stop tracking schema version compatibility, to make sure that
testing Kafka Connector would not require validating KAafka schema compatibility between different
topic versions. Kafka Connector does not allow deleting columns, renaming columns or changing column 
data type in Kinetica table, although these operations can be performed through Kinetica gAdmin tool.

Start AvroSerializerDataPump as follows: 

```sh
cd target
java -cp test-connector-jar-with-dependencies.jar com.kinetica.kafka.AvroSerializerDataPump \
     -t TestTable -c 50 -s http://localhost:8081 http://localhost:9092
```

Start Kafka Connector with the same connector and work task properties as described above for  
SchemaTestSerializerDataPump. After running the connector, make sure that Kinetica table TestTable
has been created and populated with 200 records (50 per each of 4 schemas). Verify that each of 50
record batches inserted per schema differs from other batches in the fields populated with null values.


In the AvroSerializerDataPumpSinkTest JUnit tests follow more elaborate scenarios, providing 
comparison of schemas and Kinetica tables created not only in record count, but in column count for
various combinations of config parameters and schema evolution compatibility values. Running
AvroSerializerDataPumpSinkTest does not require a running schema evolution service.



