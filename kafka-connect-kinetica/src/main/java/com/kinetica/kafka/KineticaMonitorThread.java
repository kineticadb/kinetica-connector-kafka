package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.gpudb.Avro;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableMonitorResponse;
import com.gpudb.protocol.GetRecordsResponse;

public class KineticaMonitorThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(KineticaMonitorThread.class);

    // control if record values should have a Schema
    private final boolean schemalessValues = false;

    private final GPUdb gpudb;
    private final String kineticaTable;
    private final String kafkaTopic;
    private final int version;
    private final String zmqUrl;
    private final LinkedBlockingQueue<SourceRecord> queue;

    private Type kineticaType;
    private Schema kafkaSchema;
    private String zmqTopic;
    
    /**
     * Thread monitoring inserts into Kinetica DB table and sending new records to message queue  
     * @param source         Connector source task
     * @param gpudb          Kinetica DB access object 
     * @param table          Kinetica table name
     * @param schemaVersion  Kafka topic schema version (defaults to 1)
     */
    public KineticaMonitorThread(KineticaSourceTask source, GPUdb gpudb, String table, int schemaVersion) {
        this.gpudb = gpudb;
        this.kafkaTopic = source.getTopicPrefix() + table;
        this.kineticaTable = table;
        this.zmqUrl = source.getZmqUrl();
        this.queue = source.getQueue();
        this.version = schemaVersion;
    }

    /**
     * Creates new or retrieves existing Kinetica table monitor per each table 
     * that would listen for insert statements and post new records into a message queue
     * Oly one KineticaMonitorThread per table per Kinetica DB instance exists
     */
    @Override
    public void run() {
        LOG.info("Starting monitor thread for table <{}>", this.kineticaTable);

        try {
            // Create the table monitor.
            CreateTableMonitorResponse response = this.gpudb.createTableMonitor(this.kineticaTable, null);
            this.kineticaType = new Type(response.getTypeSchema());
            this.zmqTopic = response.getTopicId();

            // Create a Kafka schema from the table type.
            this.kafkaSchema = getSchema(this.kineticaTable, this.kineticaType, this.version);

            // TODO: when kinetica.from_beginning=true, add all records existing before tableMonitor started 
            // GetRecordsResponse<GenericRecord> records = gpudb.getRecords(kineticaTable, 0, -9999, null);
            // List<GenericRecord> data = records.getData();

            // Subscribe to the ZMQ topic for the table monitor.
            try (ZMQ.Context zmqContext = ZMQ.context(1); ZMQ.Socket subscriber = zmqContext.socket(ZMQ.SUB)) {
                subscriber.connect(this.zmqUrl);
                subscriber.subscribe(this.zmqTopic.getBytes());
                subscriber.setReceiveTimeOut(1000);

                LOG.info("Starting ZMQ monitor ID=<{}> table=<{}>", this.zmqTopic, this.kineticaTable);
                processTopic(subscriber);
            }

        }
        catch (GPUdbException ex) {
            String msg = String.format("Error processing monitor <%s> for table <%s>: %s",
                    this.zmqUrl, this.kineticaTable, ex.getMessage());
            throw new ConnectException(msg, ex);
        }

        // separate try block to prevent resource leak
        try {
            LOG.info("Stopping ZMQ monitor ID=<{}> table=<{}>", this.zmqTopic, this.kineticaTable);
            this.gpudb.clearTableMonitor(this.zmqTopic, null);
        }
        catch (GPUdbException ex) {
            throw new ConnectException(ex);
        }
    }

    /**
     * Returns Kafka Schema with fields matching Kinetica table columns, name derived from Kinetica table name 
     * and assigned schema version
     * @param tableName      Kinetica table name
     * @param type           record type of the Kinetica table
     * @param schemaVersion  Kafka schema version
     * @return Kafka Schema with fields matching Kinetica table columns 
     */
    private static Schema getSchema(String tableName, Type type, int schemaVersion)  {
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName).version(schemaVersion);

        for (Column column : type.getColumns())
        {
            Schema fieldSchema;

            if (column.getType() == ByteBuffer.class) {
                fieldSchema = Schema.BYTES_SCHEMA;
            } else if (column.getType() == Double.class) {
                fieldSchema = Schema.FLOAT64_SCHEMA;
            } else if (column.getType() == Float.class) {
                fieldSchema = Schema.FLOAT32_SCHEMA;
            } else if (column.getType() == Integer.class) {
                fieldSchema = Schema.INT32_SCHEMA;
            } else if (column.getType() == Long.class) {
                fieldSchema = Schema.INT64_SCHEMA;
            } else if (column.getType() == String.class) {
                fieldSchema = Schema.STRING_SCHEMA;
            } else {
                throw new KafkaException("Unknown column type " + column.getType().getName() + ".");
            }

            builder = builder.field(column.getName(), fieldSchema);
        }
        return builder.build();
    }

    /**
     * Processes message queue received messages, looping over individual frames
     * @param subscriber         message queue socket subscriber
     * @throws ConnectException
     * @throws GPUdbException
     */
    private void processTopic(Socket subscriber) throws ConnectException, GPUdbException {
        // Loop until the thread is interrupted when the task is
        // stopped.
        long recordNumber = 0;

        while (!Thread.currentThread().isInterrupted()) {
            // Check for a ZMQ message; if none was received within
            // the timeout window continue waiting.

            ZMsg message = ZMsg.recvMsg(subscriber);
            if (message == null) {
                continue;
            }
            LOG.info("Got ZMQ message ID=<{}> table=<{}>", this.zmqTopic, this.kineticaTable);

            boolean skip = true;
            for (ZFrame frame : message) {
                // Skip the first frame (which just contains the
                // topic ID).

                if (skip) {
                    skip = false;
                    continue;
                }
                processFrame(frame, recordNumber++);
            }
        }
    }

    /**
     * Processes a message queue's single data frame, looping over individual records  
     * @param frame             message queue's data frame
     * @param recordNumber      message queue's record count
     * @throws GPUdbException
     */
    private void processFrame(ZFrame frame, long recordNumber) throws GPUdbException {
        // Create a source record for each record and put it
        // into the source record queue.
    	GenericRecord inRecord;
    	try {
        	inRecord = Avro.decode(this.kineticaType, ByteBuffer.wrap(frame.getData()));
    	} catch (GPUdbException e) {
    		// buffered message is not of expected type 
    		// if kineticaType has been changed while Connector is running, get most recent one  
    		this.kineticaType = Type.fromTable(this.gpudb, kineticaTable);
    		inRecord = Avro.decode(this.kineticaType, ByteBuffer.wrap(frame.getData()));
    	}
        final Object valueData;
        final Schema valueSchema;
        final Object keyData;
        final Schema keySchema;

        if(this.schemalessValues) {
            // follow the GoldenGate convention and use the tablename with no schema
            keySchema = null;
            keyData = this.kineticaTable;
            valueSchema = null;
            valueData = getValueMap(inRecord);
        }
        else {
            // follow the Kinetica convention and give the key a sequence with a schema
            keySchema = Schema.INT64_SCHEMA;
            keyData = new Long(recordNumber);
            valueSchema = this.kafkaSchema;
            if (this.kafkaSchema.fields().size() != inRecord.getType().getColumns().size()) {
            	LOG.info("Non-matching gpudbType and record");
            }
            valueData = getValueStruct(inRecord);
        }

        // update Kafka partition and offset, adding up to processed message counts
        Map<String,?> sourcePartition = Collections.singletonMap("table", this.kineticaTable);
        Map<String,?> sourceOffset = Collections.singletonMap("record", recordNumber);

        SourceRecord outRecord = new SourceRecord(
                sourcePartition, sourceOffset,
                this.kafkaTopic,
                keySchema, keyData,
                valueSchema, valueData);

        this.queue.add(outRecord);
    }

    /**
     * Parses data record into a hashmap object for schemaless records
     * @param inRecord    incoming generic data record
     * @return key-value pairs map
     */
    private HashMap<String, Object> getValueMap(GenericRecord inRecord) {
        HashMap<String, Object>  valueMap = new HashMap<>();

        for (Column column : this.kineticaType.getColumns()) {
            Object value = inRecord.get(column.getName());
            valueMap.put(column.getName(), value);
        }

        return valueMap;
    }

    /**
     * Creates a clean Kafka data Struct object for objects with schema
     * @param inRecord  incoming generic data record
     * @return Kafka data Struct
     */
    private Struct getValueStruct(GenericRecord inRecord) {
        Struct valueStruct = new Struct(this.kafkaSchema);

        for (Column column : this.kineticaType.getColumns()) {
            Object value = inRecord.get(column.getName());
            // safely wraps the ByteBuffer
            if (value instanceof ByteBuffer) {
                value = ((ByteBuffer)value).array();
            }
            valueStruct.put(column.getName(), value);
        }

        return valueStruct;
    }
}
