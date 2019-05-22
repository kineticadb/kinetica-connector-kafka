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
