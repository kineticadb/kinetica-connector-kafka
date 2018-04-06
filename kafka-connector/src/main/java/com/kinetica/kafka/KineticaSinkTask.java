package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;

//import kafka.common.KafkaException;

/**
 * Kafka SinkTask for streaming data into a Kinetica table.
 *
 * The data streaming pipeline will begin with records being added to the Kafka
 * topic to which the {@link KineticaSinkConnector} is attached.  As records are
 * queued, this SinkTask will connect to that queue, stream records from it, and
 * insert them into a Kinetica target table.
 *
 * The streaming target table can either be part of a collection or not, and can
 * also be a collection itself.
 */
public class KineticaSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(KineticaSinkTask.class);

    // Dates larger than this will fail in 6.1
    private final static long MAX_DATE = 29379542399999L;
    private final static long MIN_DATE = -30610224000000L;

    // cached objects
    private final HashMap<String, BulkInserter<GenericRecord>> biMap = new HashMap<>();
    private final HashMap<String, Type> typeMap = new HashMap<>();

    private final SimpleDateFormat tsFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SinkSchemaManager schemaMgr;

    @Override
    public void start(Map<String, String> props) {
        this.schemaMgr = new SinkSchemaManager(props);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

        for(BulkInserter<GenericRecord> bi : this.biMap.values()) {
            try {
                long recordsBefore = bi.getCountInserted();
                bi.flush();
                long recordsInserted = bi.getCountInserted() - recordsBefore;

                if(recordsInserted == 0) {
                    continue;
                }
                LOG.info("[{}] Flushing {} records for <{}>",
                        Thread.currentThread().getName(), recordsInserted, bi.getTableName());
            }
            catch (GPUdbException ex) {
                LOG.error("Unable to insert into table: {}", bi.getTableName(), ex);
                throw new ConnectException(ex);
            }
        }
    }

    @Override
    public void stop() {
        this.biMap.clear();
        this.typeMap.clear();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords.isEmpty()) {
            return;
        }

        // Loop through all sink records in the collection and create records
        // out of them.
        for (SinkRecord sinkRecord : sinkRecords) {

            Type gpudbSchema;
            BulkInserter<GenericRecord> builkInserter;

            try {
                builkInserter = getBulkInserter(sinkRecord);
                gpudbSchema = this.typeMap.get(builkInserter.getTableName());
            }
            catch (Exception ex) {
                KafkaException kex = new KafkaException(String.format("Unable to obtain schema: %s",
                        ex.getMessage()), ex);
                LOG.error(kex.getMessage(), ex);
                throw kex;
            }

            GenericRecord gpudbRecord;
            try {
                gpudbRecord = convertRecord(sinkRecord.value(), gpudbSchema);
                builkInserter.insert(gpudbRecord);
            }
            catch(Exception ex) {
                KafkaException kex = new KafkaException(String.format("Record conversion failed for %s: %s",
                                builkInserter.getTableName(), ex.getMessage()), ex);
                LOG.error(kex.getMessage(), ex);
                throw kex;
            }
        }

        LOG.debug("Sunk {} records", sinkRecords.size());
    }

    private BulkInserter<GenericRecord> getBulkInserter(SinkRecord record) throws Exception {
        String tableName;
        Object genericSchema = record.valueSchema();

        if(genericSchema == null) {
            // this must be a schemaless record

            Object keyValue = record.key();
            String sourceTable = null;
            if(keyValue.getClass() == String.class) {
                // for schemaless records assume the key has the table name
                sourceTable = (String)keyValue;
            }

            // get thet tablename from the key
            tableName = this.schemaMgr.getDestTable(sourceTable);

            @SuppressWarnings("unchecked")
            HashMap<String, Object> valueHash = (HashMap<String, Object>)record.value();
            genericSchema = getColumnsFromMap(valueHash);
        }
        else {
            // must be a kafka Schema
            Schema kafkaSchema = (Schema)genericSchema;

            // generate tablename from kafka schema
            tableName = this.schemaMgr.getDestTable(kafkaSchema.name());
        }

        // see if it is already in the cache
        BulkInserter<GenericRecord> bulkInserter = this.biMap.get(tableName);
        if (bulkInserter != null) {
            return bulkInserter;
        }

        // Create the bulk inserter based on the previously obtained type.
        Type gpudbSchema = this.schemaMgr.getType(tableName, genericSchema);
        this.typeMap.put(tableName, gpudbSchema);

        bulkInserter = this.schemaMgr.getBulkInserter(tableName, gpudbSchema);
        this.biMap.put(tableName, bulkInserter);

        return bulkInserter;
    }

    private GenericRecord convertRecord(Object inRecord, Type gpudbSchema) throws Exception {
        GenericRecord outRecord = new GenericRecord(gpudbSchema);
        Class<?> inRecordType = inRecord.getClass();

        // Loop through all columns.
        for (Column column : gpudbSchema.getColumns()) {
            try {
                String columnName = column.getName();
                Object inValue = getColumnValue(columnName, inRecord, inRecordType);

                Object outValue = convertValue(inValue, column);
                if(outValue == null) {
                    continue;
                }

                outRecord.put(column.getName(), outValue);
            }
            catch(Exception ex) {
                throw new Exception(String.format("Convert failed for column %s: %s",
                        column.getName(), ex.getMessage()), ex);
            }
        }
        return outRecord;
    }

    private Object getColumnValue(String columnName, Object inRecord, Class<?> inRecordType) throws Exception {
        Object inValue;

        if(inRecordType == Struct.class) {
            // we have a Kafka Schema
            Struct structRec = (Struct)inRecord;
            inValue = structRec.get(columnName);
        }
        else if(inRecordType == HashMap.class) {
            // schema-less record
            @SuppressWarnings("unchecked")
            HashMap<String, Object> hashRec = (HashMap<String, Object>)inRecord;

            hashRec = getColumnsFromMap(hashRec);
            if(!hashRec.containsKey(columnName)) {
                throw new Exception("Column is not found in record.");
            }

            inValue = hashRec.get(columnName);
        }
        else {
            throw new Exception("Record type not supported: " + inRecord.getClass().toString());
        }

        return inValue;
    }

    private Object convertValue(Object inValue, Column column) throws Exception {
        if(inValue == null) {
            if(!column.isNullable()) {
                throw new Exception("Unsupported null value in field.");
            }
            return null;
        }

        Class<?> inType = inValue.getClass();
        Class<?> outType = column.getType();
        Object outValue;

        // Check the type of the field in the sink record versus the
        // type of the column to ensure compatibility; if not
        // compatible, fail the record, otherwise copy the value.

        if(outType.isAssignableFrom(inType)) {
            // same types
            outValue = inValue;
        }
        else if(Number.class.isAssignableFrom(inType)) {
            Number inNumber = (Number)inValue;
            if(outType == Long.class) {
                outValue = inNumber.longValue();
            }
            else if(outType == Integer.class) {
                outValue = inNumber.intValue();
            }
            else if(outType == Double.class) {
                outValue = inNumber.doubleValue();
            }
            else if(outType == Float.class) {
                outValue = inNumber.floatValue();
            }
            else {
                throw new Exception(String.format("Could not convert numeric type: %s -> %s",
                            outType.getSimpleName(), inType.getSimpleName()));
            }
        }
        else if(inValue instanceof String && column.getProperties().contains("timestamp")) {
            String dateStr = (String)inValue;
            Date dateVal = this.tsFormatter.parse(dateStr);
            long outDate = dateVal.getTime();

            if (outDate > MAX_DATE) {
                outDate = MAX_DATE;
            }

            if (outDate < MIN_DATE) {
                outDate = MIN_DATE;
            }
            outValue = outDate;
        }
        else if(outType == ByteBuffer.class && inValue instanceof byte[]) {
            outValue = ByteBuffer.wrap((byte[])inValue);
        }
        else {
            throw new Exception(String.format("Type mismatch. Expected %s but got %s.",
                    outType.getSimpleName(), inType.getSimpleName()));
        }

        return outValue;
    }

    private HashMap<String, Object> getColumnsFromMap(HashMap<String, Object> columnMap) throws Exception {
        // Assume that if there is an op_type field then this is from Oracle Golden Gate.
        String oggOperation = (String)columnMap.get("op_type");
        if(oggOperation == null) {
            // This is not OGG and is a regular schema-less record.
            return columnMap;
        }

        // OGG stores the columns in the "after" field.
        @SuppressWarnings("unchecked")
        HashMap<String, Object> afterRecord = (HashMap<String, Object>)columnMap.get("after");
        if(afterRecord == null) {
            throw new Exception("OGG after record is missing.");
        }

        return afterRecord;
    }
}
