package com.kinetica.kafka;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import com.gpudb.BulkInserter.InsertException;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.AlterTableRequest;

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

    private final SimpleDateFormat tsFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    SinkSchemaManager schemaMgr;

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        this.schemaMgr = new SinkSchemaManager(props);
    }

    /**
     * Loop through all Kinetica BulkInserters in biMap to flush all inserted records 
     * that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link SinkRecord}s
     *                       passed to {@link #put}.
     */
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        for(BulkInserter<GenericRecord> bi : this.biMap.values()) {
            try {
                long recordsBefore = bi.getCountInserted();
                bi.flush();
                long recordsInserted = bi.getCountInserted() - recordsBefore;

                if(recordsInserted == 0) {
                    continue;
                }
                LOG.debug("[{}] Flushing {} records for <{}>",
                        Thread.currentThread().getName(), recordsInserted, bi.getTableName());
            }
            catch (GPUdbException ex) {
                LOG.error("Unable to insert into table: {}", bi.getTableName(), ex);
                throw new ConnectException(ex);
            }
        }
    }
    
    /**
     * Cleanup of BulkInserter and GpudbType collections before stopping this task. 
     */
    @Override
    public void stop() {
        for(BulkInserter<GenericRecord> bi : this.biMap.values()) {
            try {
                long recordsBefore = bi.getCountInserted();
                bi.flush();
                long recordsInserted = bi.getCountInserted() - recordsBefore;

                if(recordsInserted == 0) {
                    continue;
                }
                LOG.debug("[{}] Flushing {} records for <{}>",
                        Thread.currentThread().getName(), recordsInserted, bi.getTableName());
            }
            catch (GPUdbException ex) {
                LOG.error("Unable to insert into table: {}", bi.getTableName(), ex);
                throw new ConnectException(ex);
            }
        }
        this.biMap.clear();
        this.typeMap.clear();
    }

    /**
     * Return Kafka Connector version
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Process sinkRecords collection
     * @param sinkRecords  incoming Kafka records collection
     */
    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords.isEmpty()) {
            return;
        }
        LOG.debug("Number of records in a batch:" + sinkRecords.size());

        // Loop through all sink records in the collection and create records
        // out of them.
        for (SinkRecord sinkRecord : sinkRecords) {
            LOG.debug("sinkRecord " + sinkRecord + " \nkeySchema = [" + sinkRecord.keySchema() + "] \nvalueSchema = [" + sinkRecord.valueSchema() + 
                    "] \nkey = [" + sinkRecord.key() + "] \nvalue =[" + sinkRecord.value() + "]");
            Type gpudbSchema;
            BulkInserter<GenericRecord> builkInserter;
            String tableName = null;
            KineticaFieldMapper mapper;
            Integer schemaVersion;
            try {
                // lookup a matching BulkInserter object for the given record 
                builkInserter = getBulkInserter(sinkRecord);
                // lookup Kinetica schema type
                gpudbSchema = this.typeMap.get(builkInserter.getTableName());
                // extract tablename and schema version for mapper lookup
                tableName = builkInserter.getTableName();
                schemaVersion = this.schemaMgr.versionOf(sinkRecord.valueSchema());
                // lookup a KineticaFieldMapper in the schema manager by tablename/version
                // KineticaFieldMapper maps record values to columns in the Kinetica table
                mapper = this.schemaMgr.getFieldMapper(tableName, schemaVersion);
                LOG.debug("Mapper found: [" + tableName + " " + schemaVersion + "] " + mapper);
                LOG.debug("Mapped fields:" + Arrays.asList(mapper.getMapped().keySet().toArray(new String[mapper.getMapped().keySet().size()])));
                LOG.debug("Missing fields:" + Arrays.asList(mapper.getMissing().keySet().toArray(new String[mapper.getMissing().keySet().size()])));
            }
            catch (Exception ex) {
                KafkaException kex = new KafkaException(String.format("Unable to obtain schema: %s",
                        ex.getMessage()), ex);
                LOG.error(kex.getMessage(), ex);
                this.schemaMgr.addToBlackListed(tableName, sinkRecord.valueSchema());
                throw kex;
            }

            GenericRecord gpudbRecord;

            try {
                  gpudbRecord = convertRecord(sinkRecord.value(), gpudbSchema, mapper);
                builkInserter.insert(gpudbRecord);
            }
            catch (InsertException e) {
                // If the current BulkInserter failed because Kinetica table has been modified,
                // get the most up-to-date Type and syncronize local cached BulkInserter and Type.
                // if there is a deeper error, it would be rethrown from syncBulkInserter method
                try {
                    syncBulkInserter(tableName, e.getRecords());
                    syncMapper(tableName, schemaVersion, mapper);
                } catch (Exception ex) {
                    throw new ConnectException(ex);
                }
            }
            catch(Exception ex) {
                // catch and rethrow format-related exceptions
                KafkaException kex = new KafkaException(String.format("Record conversion failed for %s: %s",
                        builkInserter.getTableName(), ex.getMessage()), ex);
                LOG.error(kex.getMessage(), ex);
                throw kex;
            }
        }

        LOG.debug("Sunk {} records", sinkRecords.size());
    }

    /**
     * Return BulkInserter for a pre-existing or created on demand Kinetica table
     * @param record    incoming Kafka record
     * @return BulkInserter object
     * @throws Exception
     */
    private BulkInserter<GenericRecord> getBulkInserter(SinkRecord record) throws Exception {
        
        LOG.debug(" getBulkInserter..... ");
        
        String tableName;
        Object genericSchema = record.valueSchema();
        
        if(genericSchema == null) {
            
            LOG.debug(" getBulkInserter.....schema is null ");
            
            // this must be a schemaless record

            Object keyValue = record.key();
            String sourceTable = null;
            if(keyValue!=null && keyValue.getClass() == String.class) {
                // for schemaless records assume the key has the table name
                sourceTable = (String)keyValue;
            }

            if (sourceTable == null) {
                sourceTable = record.topic();
            }
            // get that tablename from the key
            tableName = this.schemaMgr.getDestTable(record.topic(), sourceTable);

            @SuppressWarnings("unchecked")
            HashMap<String, Object> valueHash = (HashMap<String, Object>)record.value();
            
            LOG.debug(" Value hash is " + valueHash);
            
            genericSchema = getColumnsFromMap(valueHash);
        }
        else {
            LOG.debug(" getBulkInserter.....schema is kafka " + genericSchema + " " + genericSchema.getClass());
            // must be a kafka Schema
            Schema kafkaSchema = (Schema)genericSchema;

            
            // generate tablename from kafka schema
            tableName = this.schemaMgr.getDestTable(record.topic(), kafkaSchema.name());
            LOG.debug("set the table name..."+tableName);
        }

        if (this.schemaMgr.isBlackListed(tableName, genericSchema)) {
            LOG.debug("Schema is blacklisted " + tableName );
            ConnectException ce = new ConnectException("Unable to convert records for this type, schema blacklisted " + tableName + " " + record );
            throw ce;
        }
        boolean needsSchemaMapping = this.schemaMgr.needsSchemaMapping(tableName, genericSchema);
        LOG.debug("Schema " + (needsSchemaMapping ? "needs":"does not need") + " SchemaMapping");
        
        BulkInserter<GenericRecord> bulkInserter;
        Type gpudbSchema = this.typeMap.get(tableName);
        
        if (needsSchemaMapping) {
            // in case Kinetica table and GPUdb Type have to be updated, old BulkInserter should be flushed 
            this.biMap.get(tableName).flush();
            // match the schemas and get the result of merge
            List<AlterTableRequest> alterTableRequests = this.schemaMgr.matchSchemas(tableName, genericSchema, gpudbSchema);
            
            if (alterTableRequests != null && !alterTableRequests.isEmpty()) {
                gpudbSchema = this.schemaMgr.alterTable(tableName, alterTableRequests);
            }    
            
            if (this.typeMap.get(tableName).getColumnCount() != gpudbSchema.getColumnCount()) {
                // gpudbType from local map has to be replaced with a new one
                this.typeMap.put(tableName, gpudbSchema);

                // create new bulkInserter for the new schema
                bulkInserter = this.schemaMgr.getBulkInserter(tableName, gpudbSchema);
                this.biMap.put(tableName, bulkInserter);

                // add new schema to known schemas
                this.schemaMgr.addToKnownSchemas(tableName, gpudbSchema);
                
                // need to update cached field mappers as well
                // lookup schema version
                Integer version = this.schemaMgr.versionOf(record.valueSchema());
                // lookup mapper in the schema manager by tablename/version
                KineticaFieldMapper mapper = this.schemaMgr.getFieldMapper(tableName, version);

                syncMapper(tableName, version, mapper);
            }
        } 

        // lookup bulkInserter in the local cache
        bulkInserter = this.biMap.get(tableName);
        
        if (bulkInserter != null) {
            return bulkInserter;
        } 
        
        // if schema is not in local cache, get it from schema manager
        if (gpudbSchema == null) {
            gpudbSchema = this.schemaMgr.getType(tableName, genericSchema);
            this.typeMap.put(tableName, gpudbSchema);
        }
        
        // Create the bulk inserter based on the previously obtained type.
        bulkInserter = this.schemaMgr.getBulkInserter(tableName, gpudbSchema);
        this.biMap.put(tableName, bulkInserter);

        return bulkInserter;
    }
    
    /**
     * Formats incoming Kafka record before inserting it into Kinetica table
     * @param inRecord       incoming generic record
     * @param gpudbSchema    gpudb Type for destination Kinetica table
     * @param mapper         KineticaFieldMapper of Kafka record fields into Kinetica table columns
     * @return   well-formed generic record to insert into Kinetica table
     * @throws Exception
     */
    private GenericRecord convertRecord(Object inRecord, Type gpudbSchema, KineticaFieldMapper mapper) throws Exception {
        GenericRecord outRecord = new GenericRecord(gpudbSchema);
        Class<?> inRecordType = inRecord.getClass();

        // Loop through all columns.
        for (Column column : gpudbSchema.getColumns()) {
            try {
                String columnName = column.getName();
                Object inValue = getColumnValue(columnName, inRecord, inRecordType);
                if (mapper.getMapped().keySet().contains(columnName)) {
                    // columns required by schema, should be present in record
                    
                    Object outValue = convertValue(inValue, column);
                    if(outValue == null) {
                        // if the column is required (not nullable), record can't be saved to DB and should be failed
                        throw new ConnectException("Unsupported null value in field " + column.getName());
                    }
                    outRecord.put(column.getName(), outValue);
                } 
                if (mapper.getMissing().keySet().contains(columnName)) {
                    // nullable columns missing from schema
                    outRecord.put(column.getName(), null);
                }                
                
            }
            catch(Exception ex) {
                throw new Exception(String.format("Convert failed for column %s: %s",
                        column.getName(), ex.getMessage()), ex);
            }
        }
        return outRecord;
    }

    /**
     * Formats incoming data field to fit destination column data type
     * @param columnName        field name
     * @param inRecord          incoming Kafka record
     * @param inRecordType      expected data type
     * @return well-formed value object
     * @throws ConnectException
     */
    private Object getColumnValue(String columnName, Object inRecord, Class<?> inRecordType) throws ConnectException {
        Object inValue;
        if(inRecordType == Struct.class) {
            // extract value from structure by name
            LOG.debug(" getColumnValue.....struct class " + inRecordType);
            // we have a Kafka Schema
            Struct structRec = (Struct)inRecord;
            try {
                inValue = structRec.get(columnName);
            } catch (Exception e) {
                LOG.warn("Field " + columnName + " is missing from data record");
                return null;
            }
        }
        else if(inRecordType == HashMap.class) {
            // extract value from hashmap by name
            LOG.debug(" getColumnValue.....hashmap class " + inRecordType);
            // schema-less record
            @SuppressWarnings("unchecked")
            HashMap<String, Object> hashRec = (HashMap<String, Object>)inRecord;

            hashRec = getColumnsFromMap(hashRec);
            if(!hashRec.containsKey(columnName)) {
                LOG.warn("Field " + columnName + " is missing from data record");
                //hiding for now - schema evolution allows this scenario
                //throw new ConnectException("Column is not found in record.");
                return null;
            }

            inValue = hashRec.get(columnName);
        }
        else {
            throw new ConnectException("Record type not supported: " + inRecord.getClass().toString());
        }

        return inValue;
    }

    /**
     * Formats value object according to column definition
     * @param inValue    incoming value
     * @param column     table column to fit the value in
     * @return well-formed object value
     * @throws Exception
     */
    private Object convertValue(Object inValue, Column column) throws Exception {        
        if(inValue == null) {
            // when incoming value is null
            if(!column.isNullable()) {
                // if the column is required (not nullable), record can't be saved to DB and should be failed
                throw new ConnectException(String.format("Unsupported null value in field %s: got value %s expected type %s", column.getName(), inValue, column.getType()));
            }
            //if the column is nullable, it's a valid null value to pass to DB
            return null;
        }
        // when incoming value is not null
        Class<?> inType = inValue.getClass();
        Class<?> outType = column.getType();
        Object outValue;

        // Check the type of the field in the sink record versus the
        // type of the column to ensure compatibility; if not
        // compatible, fail the record, otherwise copy the value.
        
        //LOG.debug(" ### convertValue inType/outType " + inType + "/" + outType);

        if(outType.isAssignableFrom(inType)) {
            // same types
            outValue = inValue;
        }
        else if(Number.class.isAssignableFrom(inType)) {
            // convert numbers
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
            else if(outType == String.class && column.getProperties().contains("datetime")) {
                outValue = tsFormatter.format(new Date(inNumber.longValue()));
            }
            else {
                throw new ConnectException(String.format("Could not convert numeric type: %s -> %s",
                            inType.getName(), outType.getName()));
            }
        }
        else if(inValue instanceof String && column.getProperties().contains("timestamp")) {
            // convert timestamp
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
            // convert serialized bytes
            outValue = ByteBuffer.wrap((byte[])inValue);
        }
        else if(inValue instanceof Boolean && outType == String.class) {
            // converted boolean data to String
            outValue = inValue.toString();
        }
        else {
            throw new ConnectException(String.format("Type mismatch. Expected %s but got %s.",
                    outType.getSimpleName(), inType.getSimpleName()));
        }

        return outValue;
    }

    /**
     * Removes OGG wrapper and op-type from incoming hashmap, leaving payload values
     * @param columnMap        mapped Objects (OGG standard or none)
     * @return cleaned up key-value payload hashmap
     * @throws ConnectException
     */
    private HashMap<String, Object> getColumnsFromMap(HashMap<String, Object> columnMap) throws ConnectException {
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
            throw new ConnectException("OGG after record is missing.");
        }

        return afterRecord;
    }
    
    /**
     * Gets the most recent Type for the Kinetica table and attempts to re-insert the failed records
     * @param tableName   table to be populated
     * @param records     records that failed inserting
     */
    private void syncBulkInserter(String tableName, List<?> records) {
        Type newType;
        BulkInserter<GenericRecord> bi;
        try {
            // attempt to get updated gpudbType directly from Kinetica and create a BulkInserter for it
            newType = this.schemaMgr.getGpudbType(tableName);
            bi = this.schemaMgr.getBulkInserter(tableName, newType);
        } catch (GPUdbException ge) {
            LOG.error(ge.getMessage(), ge);
            throw new ConnectException(String.format("Unable to access Kinetica table %s for %s: %s", tableName, ge.getMessage()), ge); 
        }
        
        try {
            // attempt to insert the records that failed before
            bi.insert((List<GenericRecord>)records);
            bi.flush();            
        } catch (InsertException e) {
            // catch Kinetica-related exceptions
            LOG.error(e.getMessage(), e);
            ConnectException ce = new ConnectException(String.format("Insert record into Kinetica table %s failed for %s: %s", 
                    tableName, e.getMessage()), e);            
            throw ce;

        }
        this.typeMap.put(tableName, newType);
        this.biMap.put(tableName, bi);
        this.schemaMgr.addToKnownSchemas(tableName, newType);
    }
    
    /**
     * Updates mapper for the Kinetica table
     * @param tableName   Kinetica table name
     * @param version     Schema version
     * @param mapper      mapper to be syncronized
     */
    private void syncMapper(String tableName, Integer version, KineticaFieldMapper mapper) {
        Type type = this.typeMap.get(tableName);
        if (type.getColumnCount()!=mapper.getMapped().size()) {
            // number of fields changed
            for (Column col : type.getColumns()) {
                if (! mapper.getMapped().keySet().contains(col.getName()) ) {
                    // if the column from the most recent Kinetica gpudbType is not in the mapper mapped columns
                    // it's considered missing
                    mapper.getMissing().put(col.getName(), col);
                }
            }
            this.schemaMgr.updateFieldMappers(tableName, version, mapper);
        }
    }
}
