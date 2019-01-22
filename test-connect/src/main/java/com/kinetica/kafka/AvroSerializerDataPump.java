package com.kinetica.kafka;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PatternOptionBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.kinetica.kafka.data.utils.KafkaSchemaHelpers;
import com.kinetica.kafka.data.utils.RandomGenerator;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;


public class AvroSerializerDataPump {
    
    private static String bootstrapUrl = null;
    private static String schemaRegistry = null;
    private static String topic = "SchemaTest";
    protected static int batchSize = 10;

    /** 
     * Produces a given number of Avro Serialized messages of the given schema type
     * @param schema
     * @param count
     */
    public static void produceAvroSerialized(Schema schema, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AvroSerializerDataPump.bootstrapUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", AvroSerializerDataPump.schemaRegistry);
        KafkaProducer producer = new KafkaProducer(props);

        System.out.println("\nProducing " +count + " Avro messages for schema " + schema.getJsonProp("connect.name") + " version "  + schema.getJsonProp("connect.version"));
        try {    
            for (int i=0; i<count; i++) {
                GenericRecord avroRecord = RandomGenerator.populateGenericRecord(schema);
                ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(AvroSerializerDataPump.topic, String.valueOf(i), avroRecord);
                producer.send(record);
                System.out.println(record);
                
            }
            Thread.sleep(1000);
        } catch(SerializationException e) {
          // may need to do something with it
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        finally {
          producer.flush();
          producer.close();
        }
    }
    
    /**
     * Produces a given number of Kafka messages with embedded schema for the given schema type,
     * mocking avro schema registry support 
     * @param topic          topic name
     * @param schemaVersion  version of SchemaTest object to use
     * @param count          number of records to generate
     * @return
     */
    public static List<SinkRecord> mockAvroSerialized(String topic, int schemaVersion, int count) {
        List<SinkRecord> records = new ArrayList<SinkRecord>();
        org.apache.kafka.connect.data.Schema kafkaSchema = SchemaRegistryUtils.getKafkaSchemaByVersion(schemaVersion);
        for (int i=0; i<count; i++) {
            Struct value = KafkaSchemaHelpers.populateStruct(kafkaSchema);
            SinkRecord record = new SinkRecord(topic, i, kafkaSchema.schema(), i, kafkaSchema, value, new Date().getTime());
            records.add(record);
        }
        return records;
    }
    
    /**
     * Primitive arg parser
     * @param args
     * @throws Exception
     */
    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        Option option;

        option = Option.builder("h")
                .longOpt("help")
                .desc("Show Usage")
                .build();
        options.addOption(option);

        option = Option.builder("n")
                .longOpt("batch-size")
                .desc("Number of records in a batch.")
                .argName("count")
                .type(PatternOptionBuilder.NUMBER_VALUE)
                .hasArg()
                .build();
        options.addOption(option);

        option = Option.builder("s")
                .longOpt("schema-registry")
                .desc("Schema registry location")
                .argName("url:port")
                .hasArg(true)
                .build();
        options.addOption(option);
        
        option = Option.builder("t")
                .longOpt("topic")
                .desc("Topic name")
                .argName("name")
                .hasArg(true)
                .build();
        options.addOption(option);
        // parse the command line arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse( options, args );

        if(line.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "AvroSerializerDataPump [options] URL", options );
            System.exit(1);
        }

        if(line.hasOption('n')) {
            Long intObj = (Long)line.getOptionObject('n');
            System.out.println(String.format("Batch size: %d", intObj));
            AvroSerializerDataPump.batchSize = intObj.intValue();
        }

        if(line.hasOption('s')) {
            String url = (String)line.getOptionObject('s');
            System.out.println(String.format("Schema Registry: %s", url));
            AvroSerializerDataPump.schemaRegistry = url;
        }

        if(line.hasOption('t')) {
            String topicName = (String)line.getOptionObject('t');
            System.out.println(String.format("Topic Name: %s", topicName));
            AvroSerializerDataPump.topic = topicName;
        }
        // get URL
        if(line.getArgs().length == 0) {
            throw new Exception("Missing bootstrap URL");
        }
        AvroSerializerDataPump.bootstrapUrl = line.getArgs()[0];
        System.out.println(String.format("Bootstrap URL: %s", AvroSerializerDataPump.bootstrapUrl));

    }
    
    /**
     * Integration test helper class:
     * Data pump creating Kafka topic and autogenerating messages for it
     * This topic has four different Avro serialized schema versions for the same class name registered with Schema Registry
     * During integration testing a properly configured KineticaSinkConnector is supposed to consume all messages into single 
     * Kinetica table that is being modified on the fly (adding columns and making columns nullable).
     *  
     * @param args     a list of command line arguments followed by Kafka Bootstrap URL (localhost:9092)
     */
    public static void main (String[] args) {
        
        try {
            parseArgs(args);
        }
        catch(Exception ex) {
            System.out.println("PARAM ERROR: " + ex.getMessage());
            System.exit(1);
        }
        // Create the topic and produce messages of schema version 1
        produceAvroSerialized(SchemaRegistryUtils.SCHEMA1, AvroSerializerDataPump.batchSize);
        // Update the topic configuration in Schema Registry to allow new schemas without checking backward/forward compatibility 
        SchemaRegistryUtils.setSchemaCompatibility(AvroSerializerDataPump.schemaRegistry, AvroSerializerDataPump.topic, SchemaRegistryUtils.CompatibilityLevel.NONE);
        // Produce messages of schema version 2 (fields added)
        produceAvroSerialized(SchemaRegistryUtils.SCHEMA2, AvroSerializerDataPump.batchSize);
        // Produce messages of schema version 3 (other fields removed)
        produceAvroSerialized(SchemaRegistryUtils.SCHEMA3, AvroSerializerDataPump.batchSize);
        // Produce messages of schema version 4 (more fields added)
        produceAvroSerialized(SchemaRegistryUtils.SCHEMA4, AvroSerializerDataPump.batchSize);
    }
    
}
