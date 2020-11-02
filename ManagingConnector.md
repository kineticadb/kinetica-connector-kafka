# Managing Kinetica Kafka Connector

## Standalone vs. Distributed Mode

Kafka Connector is set up with two independent properties files, one defining
_worker configuration_ for Kafka stack access and Kafka-related processes and
one defining _Connector configuration_, which covers Kinetica-related
processes and properties. Connectors and tasks are logical units of *work* and
run as processes. The process is called a **worker** in Kafka Connect. There are
two modes for running workers: *standalone mode* and *distributed mode*.
Identify which mode works best for your environment before getting started.

**Standalone mode** is useful for development, proof-of-concept and testing
Kafka Connect on a local Kafka stack. It can also be used for environments that
typically use single agents (for example, sending web server logs to Kafka).

**Distributed mode** runs Connect workers on multiple machines (nodes). These form
a Connect cluster. Kafka Connect distributes running connectors across the
cluster. You can add more nodes or remove nodes as your needs evolve.

Distributed mode is also more fault tolerant. If a node unexpectedly leaves
the cluster, Kafka Connect automatically distributes the work of that node to
other nodes in the cluster. And, because Kafka Connect stores connector
configurations, status, and offset information inside the Kafka cluster where
it is safely replicated, losing the node where a Connect worker runs does not
result in any lost data.

### Standalone mode

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

> **Note:**  Don't forget to create individual properties files for each of your
scenarios. Set a unique `name` value in `kinetica-sink.properties` or
`kinetica-source.properties` to uniquely identify the connector process.

> **Note:**  Worker tasks configurations (file `connect-standalone.properties` or
`connect-distributed.properties`) and connector configurations
(`quickstart-kinetica-sink.properties` or `quickstart-kinetica-source.properties`)
use different configuration parameters, mainly because the first set is configuring
Kafka access and the second set is configuring Kinetica execution.
Don't mix parameters from different configuration files for **standalone process**.

Configurations for sink and source connectors (`quickstart-kinetica-sink.properties`
and `quickstart-kinetica-source.properties`) also differ. Some of the sink connector
parameters are not recognized by source connector and vise versa.

When you start a connector (sink or source), it should have its own dedicated port
set with `rest.port` parameter. You can't use the same `connect-standalone.properties`
file for different connectors running simultaneously. It is not required, but
recommended to have separate configuration files named `connect-standalone-sink.properties`
and `connect-standalone-source.properties` with preset port values, such as
sink `rest.port=8090` and source `rest.port=8089`.

After the **Standalone process** is started, you can check all of the connector
activity either in terminal output or in log files (`KAFKA_HOME/logs/connect.log`
and `CONFLUENT_HOME/logs/connect.log` respectively).


### Distributed mode

When testing the connector in distributed mode, use the following syntax:

```sh
KAFKA_HOME>./bin/connect-distributed.sh config/connect-distributed.properties
```
or

```sh
CONFLUENT_HOME>bin/connect-distributed etc/kafka/connect-distributed.properties
```

After the Distributed mode process is started, you can use its `rest.port` to
deploy, start and stop multiple connectors through REST interface.

Kafka connector configuration sent in REST calls must consist of the configuration
parameters from `connect-standalone.properties` formatted as an application/json
object. For example, this is the Source connector JSON:

```bash
{
   "name":"kinetica-source-connector",
   "config":{
      "name":"kinetica-source-connector",
      "connector.class":"com.kinetica.kafka.KineticaSourceConnector",
      "tasks.max":"3",
      "kinetica.url":"http://localhost:9191",
      "kinetica.table_names":"KafkaConnectorTest"
   }
}
```

The value of "name" parameter should be consistent and is the same for outer
object (connector) and inner (connector config). It is going to be used as
connector name as part of the REST endpoint url.

### REST API to Manage Connectors

By default Kafka Connect is listening on port 8083, assuming your bootstrap
server IP is 127.0.0.1, here are the available REST syntax examples:

#### GET /connectors
Check the available connectors:

```sh
curl -X GET -H "Accept: application/json" http://127.0.0.1:8083/connectors
#response
["my-jdbc-source", "my-hdfs-sink"]
```

#### POST /connectors
Create a new connector (connector object is returned):

```sh
curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" --data '{"name":"kinetica-source-connector", "config": {"name":"kinetica-source-connector","connector.class":"com.kinetica.kafka.KineticaSourceConnector","tasks.max":"3","kinetica.url":"http://localhost:9191","kinetica.table_names":"KafkaConnectorTest"}}' http://127.0.0.1:8083/connectors
#response
{
   "name":"kinetica-source-connector",
   "config":{
      "name":"kinetica-source-connector",
      "connector.class":"com.kinetica.kafka.KineticaSourceConnector",
      "tasks.max":"3",
      "kinetica.url":"http://localhost:9191",
      "kinetica.table_names":"KafkaConnectorTest"
   },
   "tasks":[],
   "type":null
}
```

#### GET /connectors/(string:name)
Get info on existing connector (connector object is returned):

```sh
curl -X GET -H "Accept: application/json" http://127.0.0.1:8083/connectors/kinetica-source-connector
#response
{
   "name":"kinetica-source-connector",
   "config":{
      "name":"kinetica-source-connector",
      "connector.class":"com.kinetica.kafka.KineticaSourceConnector",
      "tasks.max":"3",
      "kinetica.url":"http://localhost:9191",
      "kinetica.table_names":"KafkaConnectorTest"
   },
   "tasks":[{"connector":"kinetica-source-connector","task":0}],
   "type":"source"
}
```

#### GET /connectors/(string:name)/tasks
Variation of the previous call, gets the connectors tasks collection only

```sh
curl -X GET -H "Accept: application/json" http://127.0.0.1:8083/connectors/kinetica-source-connector/tasks
#response
[
   {
      "id":{
         "connector":"kinetica-source-connector",
         "task":0
      },
      "config":{
         "kinetica.timeout":"0",
         "kinetica.password":"",
         "task.class":"com.kinetica.kafka.KineticaSourceTask",
         "kinetica.url":"http://localhost:9191",
         "kinetica.kafka_schema_version":"",
         "kinetica.table_names":"KafkaConnectorTest",
         "kinetica.topic_prefix":"",
         "kinetica.username":""
      }
   }
]
```

#### GET /connectors/(string:name)/config
Variation of the previous call, gets the connectors config only

```sh
curl -X GET -H "Accept: application/json" http://127.0.0.1:8083/connectors/kinetica-source-connector/config
#response
{
   "name":"kinetica-source-connector",
   "connector.class":"com.kinetica.kafka.KineticaSourceConnector",
   "tasks.max":"3",
   "kinetica.url":"http://localhost:9191",
   "kinetica.table_names":"KafkaConnectorTest"
}
```

#### PUT /connectors/(string:name)/config
Reconfigures the running connector (would cascade to reconfiguring its tasks). Please make
sure that you send only the "config" node from original JSON connector configuration:

```sh
curl -X PUT -H "Accept: application/json" -H "Content-Type: application/json" --data '{"name":"kinetica-source-connector","connector.class":"com.kinetica.kafka.KineticaSourceConnector","tasks.max":"10","kinetica.url":"http://localhost:9191","kinetica.table_names":"KafkaConnectorTest,KafkaConnectorTest2"}' http://127.0.0.1:8083/connectors/kinetica-source-connector/config
#response
{
   "name":"kinetica-source-connector",
   "config":{
      "name":"kinetica-source-connector",
      "connector.class":"com.kinetica.kafka.KineticaSourceConnector",
      "tasks.max":"10",
      "kinetica.url":"http://localhost:9191",
      "kinetica.table_names":"KafkaConnectorTest,KafkaConnectorTest2"
   },
   "tasks":[{"connector":"kinetica-source-connector","task":0}],
   "type":"source"
}
```

#### DELETE /connectors/(string:name)
Halts connector's tasks, delets the connector and its configuration

```sh
curl -X DELETE http://127.0.0.1:8083/connectors/kinetica-source-connector
#no response/ 204 No Content
```

For more considerations on using Kafka Connect REST API please refer to
[Confluent web site][CONFLUENT_REST_API].
[CONFLUENT_REST_API]: <https://docs.confluent.io/current/connect/references/restapi.html>
