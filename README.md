Kafka Developer Manual
======================

The following guide provides step by step instructions to get started
integrating *Kinetica* with *Kafka*.

This project is aimed to make *Kinetica* *Kafka* accessible, meaning data can be
streamed from a *Kinetica* table or to a *Kinetica* table via *Kafka Connect*.  The
custom *Kafka Source Connector* and *Sink Connector* do no additional
processing.

Source code for the connector can be found at https://github.com/KineticaDB/kinetica-connector-kafka


Connector Classes
-----------------

The two connector classes that integrate *Kinetica* with *Kafka* are:

``com.gpudb.kafka``

* ``GPUdbSourceConnector`` - A *Kafka Source Connector*, which receives a data
  stream from a *Kinetica* table monitor
* ``GPUdbSinkConnector`` - A *Kafka Sink Connector*, which receives a data
  stream from a *Kafka Source Connector* and writes it to *Kinetica*


-----


Streaming Data from KineticaGPUdb into Kafka
--------------------------------------------

The ``GPUdbSourceConnector`` can be used as-is by *Kafka Connect* to stream
data from *Kinetica* into *Kafka*. Data will be streamed in flat *Kafka Connect*
``Struct`` format with one field for each table column.  A separate *Kafka*
topic will be created for each *Kinetica* table configured.

The ``GPUdbSourceConnector`` is configured using a properties file that
accepts the following parameters:

* ``gpudb.url``: The URL of the *Kinetica* server
* ``gpudb.username`` (optional): Username for authentication
* ``gpudb.password`` (optional): Password for authentication
* ``gpudb.timeout`` (optional): Timeout in milliseconds 
* ``gpudb.table_names``: A comma-delimited list of names of tables in *Kinetica* to
  stream from
* ``topic_prefix``: The token that will be prepended to the name of each table
  to form the name of the corresponding *Kafka* topic into which records will be
  queued


-----


Streaming Data from Kafka into Kinetica
---------------------------------------

The ``GPUdbSinkConnector`` can be used as-is by *Kafka Connect* to stream
data from *Kafka* into *Kinetica*. Streamed data must be in a flat *Kafka Connect*
``Struct`` that uses only supported data types for fields (``BYTES``,
``FLOAT64``, ``FLOAT32``, ``INT32``, ``INT64``, and ``STRING``). No
translation is performed on the data and it is streamed directly into a
*Kinetica* table. The target table and collection will be created if they do
not exist.

The ``GPUdbSinkConnector`` is configured using a properties file that
accepts the following parameters:

* ``gpudb.url``: The URL of the *Kinetica* server
* ``gpudb.username`` (optional): Username for authentication
* ``gpudb.password`` (optional): Password for authentication
* ``gpudb.timeout`` (optional): Timeout in milliseconds
* ``gpudb.collection_name`` (optional): Collection to put the table in
* ``gpudb.table_name``: The name of the table in *Kinetica* to stream to
* ``gpudb.batch_size``: The number of records to insert at one time
* ``topics``: *Kafka* parameter specifying which topics will be used as sources

In the connect-standalone.properties file, add the line::
  
    value.converter.schemas.enable=true
  
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
        gpudb.url=<GPUdbServiceURL>
        gpudb.username=<GPUdbAuthenticatingUserName>
        gpudb.password=<GPUdbAuthenticatingUserPassword>
        gpudb.table_names=<GPUdbSourceTableNameA,GPUdbSourceTableNameB>
        gpudb.timeout=<GPUdbConnectionTimeoutInSeconds>
        topic_prefix=<TargetKafkaTopicNamesPrefix>

* Create a configuration file (``sink.properties``) for the sink connector::

        name=<UniqueNameOfSinkConnector>
        connector.class=com.gpudb.kafka.GPUdbSinkConnector
        tasks.max=<NumberOfKafkaToGPUdbWritingProcesses>
        gpudb.url=<GPUdbServiceURL>
        gpudb.username=<GPUdbAuthenticatingUserName>
        gpudb.password=<GPUdbAuthenticatingUserPassword>
        gpudb.collection_name=<TargetGPUdbCollectionName>
        gpudb.table_name=<GPUdbTargetTableName>
        gpudb.timeout=<GPUdbConnectionTimeoutInSeconds>
        gpudb.batch_size=<NumberOfRecordsToBatchBeforeInsert>
        topics=<SourceKafkaTopicNames>


-----


Example
-------

This example will demonstrate the *Kinetica Kafka Connector* in standalone mode.
It assumes the presence of a ``TwitterSource`` table in *Kinetica* that has records
streaming into it.

* Start Kafka locally

* Create a configuration file (``source.properties``) for the source connector::

        name=TwitterSourceConnector
        connector.class=com.gpudb.kafka.GPUdbSourceConnector
        tasks.max=1
        gpudb.url=http://localhost:9191
        gpudb.table_names=TwitterSource
        topic_prefix=Tweets.

* Create a configuration file (``sink.properties``) for the sink connector::

        name=TwitterSinkConnector
        connector.class=com.gpudb.kafka.GPUdbSinkConnector
        tasks.max=4
        gpudb.url=http://localhost:9191
        gpudb.table_name=TwitterTarget
        gpudb.batch_size=10
        topics=Tweets.TwitterSource

* Make sure the CLASSPATH environment variable includes the directory
  containing the *Kinetica Kafka Connector* jar::

        export CLASSPATH=<GPUdbKafkaConnectorDirectory>/*

* Run *Kafka Connect* from the *Kafka* installation directory:

        bin/connect-standalone.sh config/connect-standalone.properties source.properties sink.properties

This will stream data as it is inserted into the ``TwitterSource`` table into
the ``TwitterTarget`` table via *Kafka Connect*.
