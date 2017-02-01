Kafka Developer Manual
======================

The following guide provides step by step instructions to get started
integrating *Kinetica* with *Kafka*.

This project is aimed to make *Kinetica* *Kafka* accessible, meaning data can be
streamed from a *Kinetica* table or to a *Kinetica* table via *Kafka Connect*.  The
custom *Kafka Source Connector* and *Sink Connector* do no additional
processing.

Source code for the connector can be found at https://github.com/kineticadb/kinetica-connector-kafka


Connector Classes
-----------------

The two connector classes that integrate *Kinetica* with *Kafka* are:

``com.gpudb.kafka``

* ``GPUdbSourceConnector`` - A *Kafka Source Connector*, which receives a data
  stream from the database via table monitor
* ``GPUdbSinkConnector`` - A *Kafka Sink Connector*, which receives a data
  stream from a *Kafka Source Connector* and writes it to the database


-----


Streaming Data from Kinetica into Kafka
---------------------------------------

The ``GPUdbSourceConnector`` can be used as-is by *Kafka Connect* to stream
data from *Kinetica* into *Kafka*. Data will be streamed in flat *Kafka Connect*
``Struct`` format with one field for each table column.  A separate *Kafka*
topic will be created for each database table configured.

The ``GPUdbSourceConnector`` is configured using a properties file that
accepts the following parameters:

* ``gpudb.url``: The URL of the database server
* ``gpudb.username`` (optional): Username for authentication
* ``gpudb.password`` (optional): Password for authentication
* ``gpudb.timeout`` (optional): Timeout in milliseconds 
* ``gpudb.table_names``: A comma-delimited list of names of tables to stream
  from
* ``topic_prefix``: The token that will be prepended to the name of each table
  to form the name of the corresponding *Kafka* topic into which records will be
  queued

Note: This connector does not permit schema-less SinkRecords.  You, therefore, must add the following line to the connect-standalone.properties file before running connect-standalone.sh:
   
     value.converter.schemas.enable=true

-----


Streaming Data from Kafka into Kinetica
---------------------------------------

The ``GPUdbSinkConnector`` can be used as-is by *Kafka Connect* to stream
data from *Kafka* into *Kinetica*. Streamed data must be in a flat
*Kafka Connect* ``Struct`` that uses only supported data types for fields
(``BYTES``, ``FLOAT64``, ``FLOAT32``, ``INT32``, ``INT64``, and ``STRING``). No
translation is performed on the data and it is streamed directly into a table.
The target table and collection will be created if they do not exist.

The ``GPUdbSinkConnector`` is configured using a properties file that
accepts the following parameters:

* ``gpudb.url``: The URL of the database server
* ``gpudb.username`` (optional): Username for authentication
* ``gpudb.password`` (optional): Password for authentication
* ``gpudb.timeout`` (optional): Timeout in milliseconds
* ``gpudb.collection_name`` (optional): Collection to put the table in
* ``gpudb.table_name``: The name of the table to stream to
* ``gpudb.batch_size``: The number of records to insert at one time
* ``topics``: *Kafka* parameter specifying which topics will be used as sources


-----


Installation & Configuration
----------------------------

The connector provided in this project assumes launching will be done on a
server capable of submitting *Kafka* connectors in standalone mode or to a
cluster.

Two JAR files are produced by this project:

* ``kafka-connector-<ver>.jar`` - default JAR (not for use)
* ``kafka-connector-<ver>-jar-with-dependencies.jar`` - complete connector JAR

To install the connector:

* Copy the ``kafka-connector-<ver>-jar-with-dependencies.jar`` library to the
  target server

* Create a configuration file (``source.properties``) for the source connector::

        name=<UniqueNameOfSourceConnector>
        connector.class=com.gpudb.kafka.GPUdbSourceConnector
        tasks.max=1
        gpudb.url=<KineticaServiceURL>
        gpudb.username=<KineticaAuthenticatingUserName>
        gpudb.password=<KineticaAuthenticatingUserPassword>
        gpudb.table_names=<KineticaSourceTableNameA,KineticaSourceTableNameB>
        gpudb.timeout=<KineticaConnectionTimeoutInSeconds>
        topic_prefix=<TargetKafkaTopicNamesPrefix>

* Create a configuration file (``sink.properties``) for the sink connector::

        name=<UniqueNameOfSinkConnector>
        connector.class=com.gpudb.kafka.GPUdbSinkConnector
        tasks.max=<NumberOfKafkaToKineticaWritingProcesses>
        gpudb.url=<KineticaServiceURL>
        gpudb.username=<KineticaAuthenticatingUserName>
        gpudb.password=<KineticaAuthenticatingUserPassword>
        gpudb.collection_name=<TargetKineticaCollectionName>
        gpudb.table_name=<KineticaTargetTableName>
        gpudb.timeout=<KineticaConnectionTimeoutInSeconds>
        gpudb.batch_size=<NumberOfRecordsToBatchBeforeInsert>
        topics=<TopicPrefix><SourceTableName>


-----


System Test
-------------

This test will demonstrate the *Kinetica Kafka Connector* source and sink in
standalone mode.  First, you will need to create source and sink configuration
files as shown below::

**Note**: These files assume *Kinetica* is being run on your local host.  If it
is not, replace the URL with the correct one for your system

* Create a configuration file (``source.properties``) for the source connector::

        name=TwitterSourceConnector
        connector.class=com.gpudb.kafka.GPUdbSourceConnector
        tasks.max=1
        gpudb.url=http://localhost:9191
        gpudb.table_names=KafkaConnectorTest
        gpudb.timeout=1000
        topic_prefix=Tweets.
        

* Create a configuration file (``sink.properties``) for the sink connector::

        name=TwitterSinkConnector
        connector.class=com.gpudb.kafka.GPUdbSinkConnector
        tasks.max=4
        gpudb.url=http://localhost:9191
        gpudb.table_name=TwitterDest
        gpudb.timeout=1000
        gpudb.batch_size=100
        topics=Tweets.KafkaConnectorTest
        

     

     
The rest of this system test will require three terminal windows.

* In terminal 1, start *zookeeper* and *kafka*::

		cd <path/to/Kafka>
		bin/zookeeper-server-start.sh config/zookeeper.properties &
		bin/kafka-server-start.sh config/server.properties
        
* In terminal 2, start test datapump::
    
		java -cp kafka-connector-6.0.0-jar-with-dependencies.jar com.gpudb.kafka.tests.TestDataPump <gpudb url>

* In terminal 3, start kafka connector::

		export CLASSPATH=<path/to/kafka-connector-6.0.0-jar-with-dependencies.jar>
		cd <path/to/Kafka>
		bin/connect-standalone.sh config/connect-standalone.properties <source.properties> <sink.properties>