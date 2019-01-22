package com.kinetica.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/*
 * Class to generate a mock key schema
 */
public class KineticaSchemaTest {

    static final Schema statusSchemaKey;
    
    static {
        statusSchemaKey = SchemaBuilder.struct()
                .name("com.kinetica.examples.kafka.connect.CustomKey")
                .doc("Key for a twitter record")
                .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        
    }
    
    public static void main(String[] args) {
    }
}
