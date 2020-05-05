package com.kinetica.kafka.data.utils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Helper class for Schema Registry interactions, 
 * to be used when testing Avro Serialized messages with schema evolution 
 * @author nataliya tairbekov
 *
 */
public class SchemaRegistryUtils {

    public static final Schema NESTED_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"es_geoip1\",\"fields\":[" + 
            "{\"name\":\"property1\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"property_geo\",\"type\":{\"name\":\"property_geo\",\"type\":\"record\",\"fields\":["  + 
                   "{\"name\":\"cc2\",\"type\":[\"null\",\"string\"]}," + 
                   "{\"name\":\"lon\",\"type\":[\"null\",\"double\"]}," + 
                   "{\"name\":\"location\",\"type\":[\"null\",\"string\"]}," + 
                   "{\"name\":\"lat\",\"type\":[\"null\",\"double\"]}" + 
             "]}}," + 
            "{\"name\":\"property2\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"property3\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"property_ts1\",\"type\":[\"null\",\"long\"]}," + 
            "{\"name\":\"property4\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"property_ip1\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"n\",\"type\":[\"null\",\"int\"]}," + 
            "{\"name\":\"property5\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"property_ip2\",\"type\":[\"null\",\"string\"]}," + 
            "{\"name\":\"property_port1\",\"type\":[\"null\",\"int\"]}," + 
            "{\"name\":\"property6\",\"type\":{\"name\":\"property6\",\"type\":\"array\",\"items\":\"string\"}}," + 
            "{\"name\":\"property_port2\",\"type\":[\"null\",\"int\"]}," +
            "{\"name\":\"property7\",\"type\":[\"null\",\"string\"]}," +
            "{\"name\":\"property_ts2\",\"type\":[\"null\",\"long\"]}],\"connect.version\":1,\"connect.name\":\"SchemaTest\"}");

    public static final org.apache.kafka.connect.data.Schema KAFKA_GEOIP_SCHEMA = SchemaBuilder
        .struct()
        .name("property_geo")
        .optional()
        .field("cc2",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("lon",  org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("location",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("lat",  org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_CERTS_SCHEMA = SchemaBuilder
        .array(org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .name("property6")
        .optional()
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_ES_GEO1_NESTED_SCHEMA = SchemaBuilder
        .struct()
        .name("es_geoip1")
        .field("property1",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_geo", KAFKA_GEOIP_SCHEMA)
        .field("property2",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property3",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_ts1",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("property4",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_ip1",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("n",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("property5",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_ip2",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_port1",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("property6", KAFKA_CERTS_SCHEMA)            
        .field("property_port2",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("property7",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_ts2",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_GEOIP_ARRAYS_SCHEMA = SchemaBuilder
        .array(KAFKA_GEOIP_SCHEMA)
        .name("property_geo")
        .optional()
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_ES_GEO2_NESTED_SCHEMA = SchemaBuilder
        .struct()
        .name("es_geoip2")
        .field("property1",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_geo", KAFKA_GEOIP_ARRAYS_SCHEMA)
        .field("property_ts1",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("property2",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property3",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property4",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property5",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property6",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_ip1",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("n",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("property_ip2",  org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_port1",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("property_port2",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("property_ts2",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("property7",  org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE1_SCHEMA_OBJECT_B = SchemaBuilder
        .struct()
        .name("objectB")
        .field("type", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE1_SCHEMA_OBJECT_A = SchemaBuilder
        .struct()
        .name("objectA")
        .field("property_num", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("objectB", KAFKA_CASE1_SCHEMA_OBJECT_B)
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE1_SCHEMA = SchemaBuilder
        .struct()
        .name("case1")
        .field("property_ts", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_desc", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property_message", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("objectA", KAFKA_CASE1_SCHEMA_OBJECT_A)
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_STRING_OBJ_F = SchemaBuilder
        .array(org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .name("objectF")
        .optional()
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_STRING_GROUPS_SCHEMA = SchemaBuilder
        .array(org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .name("subGroupsD")
        .optional()
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_E = SchemaBuilder
        .struct()
        .name("objectE")
        .field("hasProperty1", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("hasProperty2", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_QF = SchemaBuilder
        .struct()
        .name("subObjectD")
        .field("subGroupsD", KAFKA_STRING_GROUPS_SCHEMA)
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_D = SchemaBuilder
        .struct()
        .name("objectD")
        .field("subObjectD", KAFKA_CASE2_QF)
        .field("isA", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isB", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isC", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isD", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isE", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isF", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isG", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isH", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("isI", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)            
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_C = SchemaBuilder
        .struct()
        .name("objectC")
        .field("objectCproperty", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_B_COLLECTION_ITEM = SchemaBuilder
        .struct()
        .name("objectBcollection")
        .field("collection_item1", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item2", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item3", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item4", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("collection_item5", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item6", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item7", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item8", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("collection_item9", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("collection_item10", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .build();
            
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_B_COLLECTION = SchemaBuilder
        .array(KAFKA_CASE2_OBJ_B_COLLECTION_ITEM)
        .name("objectBcollection")
        .optional()
        .build();
            
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_B = SchemaBuilder
        .struct()
        .name("objectB")
        .field("objectBcollection", KAFKA_CASE2_OBJ_B_COLLECTION)
        .field("isObjectB", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
    
    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_A = SchemaBuilder
        .struct()
        .name("objectA")
        .field("propertyA1", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("propertyA2", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("propertyA3", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("propertyA4", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_OBJ_DETAIL = SchemaBuilder
        .struct()
        .name("top_object_details")
        .field("property1", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property2", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("objectA", KAFKA_CASE2_OBJ_A)
        .field("objectB", KAFKA_CASE2_OBJ_B)
        .field("objectC", KAFKA_CASE2_OBJ_C)
        .field("objectD", KAFKA_CASE2_OBJ_D)
        .field("objectE", KAFKA_CASE2_OBJ_E)
        .field("property3", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("objectF", KAFKA_STRING_OBJ_F)
        .field("property4", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("property5", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property6", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("property7", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    public static final org.apache.kafka.connect.data.Schema KAFKA_CASE2_SCHEMA = SchemaBuilder
        .struct()
        .name("case2")
        .field("top_object_id", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("top_object_details", KAFKA_CASE2_OBJ_DETAIL)
        .build();
    
    public static final Schema TICKER_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"TickerTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"securityType\",\"type\":[\"null\",\"string\"]},{\"name\":\"bidPrice\",\"type\":[\"null\",\"float\"]},{\"name\":\"bidSize\",\"type\":[\"null\",\"int\"]},{\"name\":\"askPrice\",\"type\":[\"null\",\"float\"]},{\"name\":\"askSize\",\"type\":[\"null\",\"int\"]},{\"name\":\"lastUpdated\",\"type\":\"long\"},{\"name\":\"lastSalePrice\",\"type\":[\"null\",\"float\"]},{\"name\":\"lastSaleSize\",\"type\":[\"null\",\"int\"]},{\"name\":\"lastSaleTime\",\"type\":\"long\"},{\"name\":\"volume\",\"type\":[\"null\",\"int\"]},{\"name\":\"marketPercent\",\"type\":[\"null\",\"float\"]}],\"connect.version\":1,\"connect.name\":\"TickerTest\"}");

    public static final org.apache.kafka.connect.data.Schema KAFKA_TICKER_SCHEMA 
        = SchemaBuilder.struct()
        .name("com.kinetica.kafka.TickerTest")
        .field("symbol", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("securityType", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("bidPrice", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("bidSize", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("askPrice", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("askSize", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("lastUpdated", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("lastSalePrice", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("lastSaleSize", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("lastSaleTime", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("volume", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("marketPercent", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .build();
    
    public static final String TICKER_PK = TICKER_SCHEMA.getFields().get(0).name();

    public static final Schema SCHEMA1 = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"SchemaTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"test_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_float\",\"type\":[\"null\",\"float\"]},{\"name\":\"test_double\",\"type\":[\"null\",\"double\"]},{\"name\":\"test_bytes\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"test_string\",\"type\":[\"null\",\"string\"]}],\"connect.version\":1,\"connect.name\":\"SchemaTest\"}");

    public static final org.apache.kafka.connect.data.Schema KAFKA_SCHEMA_1 
        = SchemaBuilder.struct()
        .name("com.kinetica.kafka.SchemaTest")
        .version(1)
        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("timestamp", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("test_int", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("test_long", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("test_float", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("test_double", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("test_bytes", org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
        .field("test_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    public static final Schema SCHEMA2 = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"SchemaTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"test_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_float\",\"type\":[\"null\",\"float\"]},{\"name\":\"test_double\",\"type\":[\"null\",\"double\"]},{\"name\":\"test_bytes\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"test_string\",\"type\":[\"null\",\"string\"]},{\"name\":\"test_int2\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long2\",\"type\":[\"null\",\"long\"]}],\"connect.version\":2,\"connect.name\":\"SchemaTest\"}");

    public static final org.apache.kafka.connect.data.Schema KAFKA_SCHEMA_2 
        = SchemaBuilder.struct()
        .name("com.kinetica.kafka.SchemaTest")
        .version(2)
        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("timestamp", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("test_int", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("test_long", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("test_float", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("test_double", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("test_bytes", org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
        .field("test_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("test_int2", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("test_long2", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    public static final Schema SCHEMA3 = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"SchemaTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"test_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_double\",\"type\":[\"null\",\"double\"]},{\"name\":\"test_bytes\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"test_string\",\"type\":[\"null\",\"string\"]}],\"connect.version\":3,\"connect.name\":\"SchemaTest\"}");

    public static final org.apache.kafka.connect.data.Schema KAFKA_SCHEMA_3 
        = SchemaBuilder.struct()
        .name("com.kinetica.kafka.SchemaTest")
        .version(3)
        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("timestamp", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("test_int", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .field("test_long", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("test_double", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .field("test_bytes", org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
        .field("test_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    public static final Schema SCHEMA4 = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"SchemaTest\",\"namespace\":\"com.kinetica.kafka\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"test_int\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_float\",\"type\":[\"null\",\"float\"]},{\"name\":\"test_double\",\"type\":[\"null\",\"double\"]},{\"name\":\"test_bytes\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"test_string\",\"type\":[\"null\",\"string\"]},{\"name\":\"test_int2\",\"type\":[\"null\",\"int\"]},{\"name\":\"test_long2\",\"type\":[\"null\",\"long\"]},{\"name\":\"test_float2\",\"type\":[\"null\",\"float\"]},{\"name\":\"test_double2\",\"type\":[\"null\",\"double\"]}],\"connect.version\":4,\"connect.name\":\"SchemaTest\"}");

    public static final org.apache.kafka.connect.data.Schema KAFKA_SCHEMA_4 
        = SchemaBuilder.struct()
        .name("com.kinetica.kafka.SchemaTest")
        .version(4)
        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("timestamp", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("test_int", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("test_long", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("test_float", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("test_double", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("test_bytes", org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
        .field("test_string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("test_int2", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
        .field("test_long2", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("test_float2", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("test_double2", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    public static Schema getSchemaByVersion(int version) {
        switch(version) {
            case 1: return SCHEMA1;
            case 2: return SCHEMA2;
            case 3: return SCHEMA3;
            case 4: return SCHEMA4;
            default: 
                    return null;
        }
    }

    public static org.apache.kafka.connect.data.Schema getKafkaSchemaByVersion(int version) {
        switch (version) {
        case 1: return KAFKA_SCHEMA_1;
        case 2: return KAFKA_SCHEMA_2;
        case 3: return KAFKA_SCHEMA_3;
        case 4: return KAFKA_SCHEMA_4;
        default:
                return null;
        }
    }
    
    public enum CompatibilityLevel{
        NONE, BACKWARD, FORWARD, BOTH
    }
    
    /**
//     * Connects to Kafka Schema Registry and sets topic compatibility level to provided value
     * @param schemaUrl       Schema Registry url (localhost:8081) 
     * @param topic           topic to reconfigure
     * @param compatibility   compatibility level
     */
    public static void setSchemaCompatibility(String schemaUrl, String topic, CompatibilityLevel compatibility) {
        System.out.println("\nForcing Kafka topic schema evolution compatibility to [" + compatibility.name() + "]");
        URL url = null; 
        HttpURLConnection connection = null;
        BufferedReader reader = null;
        StringBuilder stringBuilder;

        byte[] postData = ("{\"compatibility\": \"" + compatibility.name() + "\"}").getBytes();
        try {
            url = new URL(schemaUrl + "/config/" + topic + "-value/");
            connection = (HttpURLConnection)url.openConnection();
            connection.setConnectTimeout(15000);
            connection.setReadTimeout(15000);
            connection.setRequestMethod("PUT");
            connection.setInstanceFollowRedirects( true );
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-type", "application/vnd.schemaregistry.v1+json");
            connection.setRequestProperty( "charset", "utf-8");
            connection.setRequestProperty( "Content-Length", Integer.toString( postData.length ));
            connection.setUseCaches( false );
            try( DataOutputStream wr = new DataOutputStream( connection.getOutputStream())) {
                wr.write( postData );
            }
    
            connection.connect();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            stringBuilder = new StringBuilder();
            
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line + "\n");
            }
            System.out.println("Received response: " + stringBuilder.toString());
            Thread.sleep(1000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // close the reader; this can throw an exception too, so
            // wrap it in another try/catch block.
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
    }

}
