package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;

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

    // from configuration
    private String collectionName;
    private GPUdb gpudb;
    private int batchSize;
    private String tablePrefix;

    // cached objects
    private final HashMap<String, Type> typeMap = new HashMap<>();
    private final HashMap<String, BulkInserter<GenericRecord>> biMap = new HashMap<>();

    @Override
    public void start(Map<String, String> props) {
        String url = props.get(KineticaSinkConnector.URL_CONFIG);
        this.batchSize = Integer.parseInt(props.get(KineticaSinkConnector.BATCH_SIZE_CONFIG));
        this.collectionName = props.get(KineticaSinkConnector.COLLECTION_NAME_CONFIG);
        this.tablePrefix = props.get(KineticaSinkConnector.TABLE_PREFIX_CONFIG);

        try {
            this.gpudb = new GPUdb(url, new GPUdb.Options()
                    .setUsername(props.get(KineticaSinkConnector.USERNAME_CONFIG))
                    .setPassword(props.get(KineticaSinkConnector.PASSWORD_CONFIG))
                    .setTimeout(Integer.parseInt(props.get(KineticaSinkConnector.TIMEOUT_CONFIG))));
        }
        catch (GPUdbException ex) {
            ConnectException cex = new ConnectException("Unable to connect to Kinetica at: " + url, ex);
            LOG.error(cex.getMessage(), ex);
            throw cex;
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

        for(BulkInserter<GenericRecord> bi : this.biMap.values()) {
            try {
                long recordsBefore = bi.getCountInserted();
                bi.flush();
                long recordsInserted = bi.getCountInserted() - recordsBefore;
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

        Schema kafkaSchema = sinkRecords.iterator().next().valueSchema();
        Type gpudbSchema;
        BulkInserter<GenericRecord> builkInserter;
        String tableName;

        try {
            // create table to get that type if necessary
            tableName = this.tablePrefix + kafkaSchema.name();
            gpudbSchema = getType(kafkaSchema, tableName);

            // Create the bulk inserter based on the previously obtained type.
            builkInserter = getBulkInserter(gpudbSchema, tableName);
        }
        catch (GPUdbException gex) {
            ConnectException kex = new ConnectException(gex.getMessage(), gex);
            LOG.error(kex.getMessage(), kex);
            throw kex;
        }
        catch (Exception ex) {
            KafkaException kex = new KafkaException(ex.getMessage(), ex);
            LOG.error(kex.getMessage(), kex);
            throw kex;
        }

        // Loop through all sink records in the collection and create records
        // out of them.
        for (SinkRecord sinkRecord : sinkRecords) {

            GenericRecord record;
            try {
                record = convertRecord(sinkRecord, gpudbSchema);
            }
            catch(Exception ex) {
                KafkaException kex = new KafkaException(
                        String.format("Record conversion failed for %s: %s", tableName, ex.getMessage()), ex);
                LOG.error(kex.getMessage(), ex);
                throw kex;
            }

            // If the record didn't fail, insert into GPUdb.
            try {
                builkInserter.insert(record);
            }
            catch (GPUdbException ex) {
                KafkaException kex = new KafkaException(
                        String.format("Unable to insert into %s: %s", tableName, ex.getMessage()), ex);
                LOG.error(kex.getMessage(), ex);
                throw kex;
            }
        }

        LOG.debug("Sunk {} records", sinkRecords.size());
    }

    BulkInserter<GenericRecord> getBulkInserter(Type gpudbSchema, String tableName) throws GPUdbException {

        BulkInserter<GenericRecord> cachedBi = this.biMap.get(tableName);
        if (cachedBi != null) {
            return cachedBi;
        }

        // If there is no bulk inserter yet (because this is the first time put
        // was called), either look up the type from the specified table (if it
        // exists) or create one (and the table) by analyzing the first sink
        // record in the collection, then create the bulk inserter.
        cachedBi = new BulkInserter<>(this.gpudb, tableName, gpudbSchema, this.batchSize, null);
        this.biMap.put(tableName, cachedBi);
        return cachedBi;
    }

    GenericRecord convertRecord(SinkRecord sinkRecord, Type schemaType) throws Exception {
        Struct struct = (Struct)sinkRecord.value();
        GenericRecord record = new GenericRecord(schemaType);

        // Loop through all columns.
        for (Column column : schemaType.getColumns()) {
            Object value;
            try {
                value = getColumnValue(struct, column);
            }
            catch(Exception ex) {
                throw new Exception(String.format("Convert failed for column {}: {}", column.getName(), ex.getMessage()));
            }
            record.put(column.getName(), value);
        }

        return record;
    }

    Object getColumnValue(Struct struct, Column column) throws Exception {

        String columnName = column.getName();
        Class<?> columnType = column.getType();

        Object outValue = null;
        outValue = struct.get(columnName);

        if (outValue == null) {
            throw new Exception("Unsupported null value in field.");
        }

        // Check the type of the field in the sink record versus the
        // type of the column to ensure compatibility; if not
        // compatible, fail the record, otherwise copy the value.

        if (columnType == ByteBuffer.class && outValue instanceof byte[]) {
            return ByteBuffer.wrap((byte[])outValue);
        }
        else if (columnType == Double.class && outValue instanceof Double) {
            return outValue;
        }
        else if (columnType == Float.class && outValue instanceof Float) {
            return outValue;
        }
        else if (columnType == Integer.class && outValue instanceof Integer) {
            return outValue;
        }
        else if (columnType == Long.class && outValue instanceof Long) {
            return outValue;
        }
        else if (columnType == String.class && outValue instanceof String) {
            return outValue;
        }
        else {
            throw new Exception(String.format("Type mismatch. Expected %s but got %s.",
                    columnType.getSimpleName(), outValue.getClass().getSimpleName()));
        }
    }

    Type getType(Schema schema, String tableName) throws Exception {
        Type cachedType = this.typeMap.get(tableName);
        if(cachedType != null) {
            return cachedType;
        }

        if (this.gpudb.hasTable(tableName, null).getTableExists()) {
            // If the table exists, get the schema from it.
            LOG.info("Creating type for table: {}", tableName);
            cachedType = Type.fromTable(this.gpudb, tableName);
            this.typeMap.put(tableName, cachedType);
            return cachedType;
        }

        cachedType = createTable(schema, tableName);
        this.typeMap.put(tableName, cachedType);
        return cachedType;
    }

    Type createTable(Schema schema, String tableName) throws Exception {
        // If the table does not exist, loop through the first sink record's
        // fields and build a type based on their names and types.
        List<Column> columns = new ArrayList<>();

        for (Field field : schema.fields()) {
            if (null != field.schema().type()) switch (field.schema().type()) {
            case BYTES:
                columns.add(new Column(field.name(), ByteBuffer.class));
                break;

            case FLOAT64:
                columns.add(new Column(field.name(), Double.class));
                break;

            case FLOAT32:
                columns.add(new Column(field.name(), Float.class));
                break;

            case INT32:
                columns.add(new Column(field.name(), Integer.class));
                break;

            case INT64:
                columns.add(new Column(field.name(), Long.class));
                break;

            case STRING:
                columns.add(new Column(field.name(), String.class));
                break;

            default:
                Exception ex = new Exception("Unsupported type for field " + field.name() + ".");
                LOG.error(ex.getMessage());
                throw ex;
            }
        }

        if (columns.isEmpty()) {
            throw new Exception("Schema has no fields.");
        }

        // Create the table based on the newly created schema.
        Type tableType = new Type(columns);
        String typeId = tableType.create(this.gpudb);

        LOG.info("Creating table: {}", tableName);
        this.gpudb.createTable(tableName, typeId,
                GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, this.collectionName) );

        return tableType;
    }
}
