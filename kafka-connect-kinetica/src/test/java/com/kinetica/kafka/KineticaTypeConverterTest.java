package com.kinetica.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.Type.Column.ColumnType;

public class KineticaTypeConverterTest {
    
    private final static Logger LOG = LoggerFactory.getLogger(KineticaSinkTaskTest.class);

    @Test
    public void simpleStructKafkaSchemaTest() throws Exception {
        String tableName = "test_table1";
        
        // This is a flat record struct with primitive types in it
        Schema kafkaSchema = SchemaBuilder.struct()
            .name("com.kinetica.kafka.StructTest")
            .field("test_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("test_int", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
            .field("test_long", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
            .field("test_float", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("test_double", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("test_boolean", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
            .build();
        KineticaFieldMapper mapper = new KineticaFieldMapper(tableName, 1);
        
        com.gpudb.Type type = KineticaTypeConverter.convertTypeFromSchema(kafkaSchema, mapper);
        
        // Evaluating the schema
        assertNotNull(type);
        assertEquals(type.getColumnCount(), 6);
        
        assertEquals(type.getColumn(0).getName(), "test_string");
        assertEquals(type.getColumn(0).getColumnType(), ColumnType.STRING);
        
        assertEquals(type.getColumn(1).getName(), "test_int");
        assertEquals(type.getColumn(1).getColumnType(), ColumnType.INTEGER);

        assertEquals(type.getColumn(2).getName(), "test_long");
        assertEquals(type.getColumn(2).getColumnType(), ColumnType.LONG);
        
        assertEquals(type.getColumn(3).getName(), "test_float");
        assertEquals(type.getColumn(3).getColumnType(), ColumnType.FLOAT);

        assertEquals(type.getColumn(4).getName(), "test_double");
        assertEquals(type.getColumn(4).getColumnType(), ColumnType.DOUBLE);

        assertEquals(type.getColumn(5).getName(), "test_boolean");
        assertEquals(type.getColumn(5).getColumnType(), ColumnType.INTEGER);
        
        // Evaluate the Mapper
        assertEquals(mapper.getMapped().size(), 6);
        // Assert that all original fields are mapped by name
        for (int i=0; i < type.getColumnCount(); i++) {
            assertTrue(mapper.getMapped().containsKey(type.getColumn(i).getName()));
        }
        
    }

    @Test
    public void nestedStructKafkaSchemaTest() throws Exception {
        String tableName = "test_table1";
        String fieldNameDelimiter = "."; 
        KineticaSinkConnectorConfig.ARRAY_FLATTENING arrayFlatteningMode = KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING;
        
        String arrayValueSeparator = ",";
        
        // This is a struct that contains both primitive type fields and a field that's a record itself (nested type) 
        Schema kafkaSchema = SchemaBuilder.struct()
            .name("com.kinetica.kafka.NestedStructTest")
                .field("test_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                .field("test_int", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                .field("test_long", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
                .field("test_float", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
                .field("test_double", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("test_boolean", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                .field("nested_struct", 
                    SchemaBuilder.struct()
                    .name("nested_struct")
                    .field("another_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                    .field("another_int", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)                    
                    .field("another_boolean", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                    .build())
                .build();
        KineticaFieldMapper mapper = new KineticaFieldMapper(tableName, 1);
        
        com.gpudb.Type type = KineticaTypeConverter.convertNestedTypeFromSchema(kafkaSchema, 
                fieldNameDelimiter, arrayFlatteningMode, arrayValueSeparator, mapper);
        
        // Evaluating the schema
        assertNotNull(type);
        assertEquals(type.getColumnCount(), 9);
        
        assertEquals(type.getColumn(0).getName(), "test_string");
        assertEquals(type.getColumn(0).getColumnType(), ColumnType.STRING);
        
        assertEquals(type.getColumn(1).getName(), "test_int");
        assertEquals(type.getColumn(1).getColumnType(), ColumnType.INTEGER);

        assertEquals(type.getColumn(2).getName(), "test_long");
        assertEquals(type.getColumn(2).getColumnType(), ColumnType.LONG);
        
        assertEquals(type.getColumn(3).getName(), "test_float");
        assertEquals(type.getColumn(3).getColumnType(), ColumnType.FLOAT);

        assertEquals(type.getColumn(4).getName(), "test_double");
        assertEquals(type.getColumn(4).getColumnType(), ColumnType.DOUBLE);

        assertEquals(type.getColumn(5).getName(), "test_boolean");
        assertEquals(type.getColumn(5).getColumnType(), ColumnType.INTEGER);
        
        assertEquals(type.getColumn(6).getName(), "nested_struct.another_string");
        assertEquals(type.getColumn(6).getColumnType(), ColumnType.STRING);
        
        assertEquals(type.getColumn(7).getName(), "nested_struct.another_int");
        assertEquals(type.getColumn(7).getColumnType(), ColumnType.INTEGER);

        assertEquals(type.getColumn(8).getName(), "nested_struct.another_boolean");
        assertEquals(type.getColumn(8).getColumnType(), ColumnType.INTEGER);

        
        // Evaluate the Mapper
        assertEquals(mapper.getMapped().size(), 9);
        // Assert that all original fields are mapped by name
        for (int i=0; i < type.getColumnCount(); i++) {
            assertTrue(mapper.getLookup().containsKey(type.getColumn(i).getName()));
        }
        
    }

    @Test
    public void simpleStructAvroSchemaTest() throws Exception {
        String tableName = "test_table2";
        org.apache.avro.Schema kafkaSchema = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"StructTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"test_string\",\"type\":[\"null\",\"string\"]},{\"name\":\"test_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_float\",\"type\":[\"null\",\"float\"]},{\"name\":\"test_double\",\"type\":[\"null\",\"double\"]},{\"name\":\"test_boolean\",\"type\":[\"null\",\"boolean\"]}]}],\"connect.version\":1,\"connect.name\":\"StructTest\"}");

        KineticaFieldMapper mapper = new KineticaFieldMapper(tableName, 1);
        
        com.gpudb.Type type = KineticaTypeConverter.convertTypeFromAvroSchema(kafkaSchema, mapper);
        
        // Evaluating the schema
        assertNotNull(type);
        assertEquals(type.getColumnCount(), 6);
        
        assertEquals(type.getColumn(0).getName(), "test_string");
        assertEquals(type.getColumn(0).getColumnType(), ColumnType.STRING);
        
        assertEquals(type.getColumn(1).getName(), "test_int");
        assertEquals(type.getColumn(1).getColumnType(), ColumnType.INTEGER);

        assertEquals(type.getColumn(2).getName(), "test_long");
        assertEquals(type.getColumn(2).getColumnType(), ColumnType.LONG);
        
        assertEquals(type.getColumn(3).getName(), "test_float");
        assertEquals(type.getColumn(3).getColumnType(), ColumnType.FLOAT);

        assertEquals(type.getColumn(4).getName(), "test_double");
        assertEquals(type.getColumn(4).getColumnType(), ColumnType.DOUBLE);

        assertEquals(type.getColumn(5).getName(), "test_boolean");
        assertEquals(type.getColumn(5).getColumnType(), ColumnType.INTEGER);
        
        // Evaluate the Mapper
        assertEquals(mapper.getMapped().size(), 6);
        // Assert that all original fields are mapped by name
        for (int i=0; i < type.getColumnCount(); i++) {
            assertTrue(mapper.getMapped().containsKey(type.getColumn(i).getName()));
        }

    }

    @Test
    public void nestedStructAvroSchemaTest() throws Exception {
        String tableName = "test_table2";
        org.apache.avro.Schema kafkaSchema = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"StructTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"test_string\",\"type\":[\"null\",\"string\"]},{\"name\":\"test_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_float\",\"type\":[\"null\",\"float\"]},{\"name\":\"test_double\",\"type\":[\"null\",\"double\"]},{\"name\":\"test_boolean\",\"type\":[\"null\",\"boolean\"]},{\"name\":\"nested_struct\", \"type\": {\"type\":\"record\", \"name\": \"another_struct\", \"fields\":[{\"name\":\"another_string\",\"type\":[\"null\",\"string\"]},{\"name\":\"another_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"another_boolean\", \"type\":[\"null\",\"boolean\"]}]}}],\"connect.version\":1,\"connect.name\":\"StructTest\"}");

        KineticaFieldMapper mapper = new KineticaFieldMapper(tableName, 1);
        
        com.gpudb.Type type = KineticaTypeConverter.convertNestedTypeFromAvroSchema(
                kafkaSchema, 
                KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER, 
                KineticaSinkConnectorConfig.ARRAY_FLATTENING.CONVERT_TO_STRING, 
                KineticaSinkConnectorConfig.DEFAULT_ARRAY_VALUE_SEPARATOR, 
                mapper);

        // Evaluating the schema
        assertNotNull(type);
        assertEquals(type.getColumnCount(), 9);
        
        assertEquals(type.getColumn(0).getName(), "test_string");
        assertEquals(type.getColumn(0).getColumnType(), ColumnType.STRING);
        
        assertEquals(type.getColumn(1).getName(), "test_int");
        assertEquals(type.getColumn(1).getColumnType(), ColumnType.INTEGER);

        assertEquals(type.getColumn(2).getName(), "test_long");
        assertEquals(type.getColumn(2).getColumnType(), ColumnType.LONG);
        
        assertEquals(type.getColumn(3).getName(), "test_float");
        assertEquals(type.getColumn(3).getColumnType(), ColumnType.FLOAT);

        assertEquals(type.getColumn(4).getName(), "test_double");
        assertEquals(type.getColumn(4).getColumnType(), ColumnType.DOUBLE);

        assertEquals(type.getColumn(5).getName(), "test_boolean");
        assertEquals(type.getColumn(5).getColumnType(), ColumnType.INTEGER);
        
        assertEquals(
        	type.getColumn(6).getName(), 
        	"nested_struct" + KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER + "another_string");
        assertEquals(type.getColumn(6).getColumnType(), ColumnType.STRING);
        
        assertEquals(
        	type.getColumn(7).getName(), 
        	"nested_struct" + KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER + "another_int");
        assertEquals(type.getColumn(7).getColumnType(), ColumnType.INTEGER);

        assertEquals(
        	type.getColumn(8).getName(), 
        	"nested_struct" + KineticaSinkConnectorConfig.DEFAULT_FIELD_NAME_DELIMITER + "another_boolean");
        assertEquals(type.getColumn(8).getColumnType(), ColumnType.INTEGER);

        // Evaluate the Mapper
        assertEquals(mapper.getMapped().size(), 9);
        // Assert that all original fields are mapped by name
        for (int i=0; i < type.getColumnCount(); i++) {
            assertTrue(mapper.getLookup().containsKey(type.getColumn(i).getName()));
        }

    }
    
    @Test
    public void simpleJsonSchemaTest() throws Exception {
        String tableName = "test_table3";
        
        HashMap <String, Object> schema = new HashMap<String, Object>();
        schema.put("test_string", "Testing string value");
        schema.put("test_int", new Integer(1));
        schema.put("test_long", new Long(1L));
        schema.put("test_float", new Float(2.5));
        schema.put("test_double", new Double(2.5d));
        schema.put("test_boolean", new Boolean(true));
        
        KineticaFieldMapper mapper = new KineticaFieldMapper(tableName, 1);
        
        com.gpudb.Type type = KineticaTypeConverter.convertTypeFromMap(schema, mapper);
        
        // Evaluating the schema
        assertNotNull(type);
        assertEquals(type.getColumnCount(), 6);
        
        // Assert that all column names are from original HashMap keyset
        for (int i=0; i < type.getColumnCount(); i++) {
            assertTrue(schema.containsKey(type.getColumn(i).getName()));
        }
        
        assertEquals(type.getColumn("test_string").getColumnType(), ColumnType.STRING);
        assertEquals(type.getColumn("test_int").getColumnType(), ColumnType.INTEGER);
        assertEquals(type.getColumn("test_long").getColumnType(), ColumnType.LONG);
        assertEquals(type.getColumn("test_float").getColumnType(), ColumnType.FLOAT);
        assertEquals(type.getColumn("test_double").getColumnType(), ColumnType.DOUBLE);
        assertEquals(type.getColumn("test_boolean").getColumnType(), ColumnType.INTEGER);

        // Evaluate the Mapper
        assertEquals(mapper.getMapped().size(), 6);
        // Assert that all original fields are mapped by name
        for (int i=0; i < type.getColumnCount(); i++) {
            assertTrue(mapper.getMapped().containsKey(type.getColumn(i).getName()));
        }

    }

}
