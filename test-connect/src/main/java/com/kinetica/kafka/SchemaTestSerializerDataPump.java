package com.kinetica.kafka;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.kinetica.kafka.data.Employee;
import com.kinetica.kafka.data.utils.KafkaSchemaHelpers;
import com.kinetica.kafka.data.utils.RandomGenerator;
import com.kinetica.kafka.data.utils.SchemaRegistryUtils;


public class SchemaTestSerializerDataPump {
    
    // command line params
    private static String bootstrapUrl = null;
    private static String schemaRegistry = null;
    private static int count = 10;
    private static int version;
    
    public static Random random = new Random();

    /** 
     * Produces a default number of Avro Serialized messages of the given schema type
     * @param schema
     */
    public static void produceAvroSerialized(Schema schema) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SchemaTestSerializerDataPump.bootstrapUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", SchemaTestSerializerDataPump.schemaRegistry);
        KafkaProducer producer = new KafkaProducer(props);
    
    
        try {
            for (int i=0; i<SchemaTestSerializerDataPump.count; i++) {
                GenericRecord avroRecord = RandomGenerator.populateGenericRecord(schema);
                String key = "key" + i;
                ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("SchemaTest", key, avroRecord);
                producer.send(record);
                System.out.println(record);
            }
        } catch(SerializationException e) {
          // may need to do something with it
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
     * Creates a record of given Schema type and populates with randomly generated values 
     * @param schema
     * @return
     */
    private static GenericRecord populateEmployee(Schema schema) {
        Employee record = Employee.newBuilder()
                .setName(RandomGenerator.getRandomString(16))
                .setJoiningDate(RandomGenerator.getFormattedRandomDate())
                .setDept(RandomGenerator.getRandomString(8))
                .setRole(RandomGenerator.getRandomString(8))
                .setSalary(RandomGenerator.random.nextFloat())
                .setBonus(RandomGenerator.random.nextFloat())
                .build();
        return record;
    }
    
    /**
     * Creates a topic "Employee" and produces a default number of Avro Serialized messages of Employee type
     */
    public static void produceEmployeeSerialized() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SchemaTestSerializerDataPump.bootstrapUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", SchemaTestSerializerDataPump.schemaRegistry);
        KafkaProducer producer = new KafkaProducer(props);
    
        Schema schema = Employee.getClassSchema();
        
        System.out.println("Producing records");
        try {
            for (int i=0; i<SchemaTestSerializerDataPump.count; i++) {
                GenericRecord avroRecord = populateEmployee(schema);
                String key = "key" + i;
                ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("Employee", key, avroRecord);
                producer.send(record);
                System.out.println(record);
            }
        } catch(SerializationException e) {
          // may need to do something with it
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
        if (1<= schemaVersion && schemaVersion <= 4) {
            // create serialized object with embedded versioned schema
            for (int i=0; i<count; i++) {
                Struct value = KafkaSchemaHelpers.populateStruct(kafkaSchema);
                SinkRecord record = new SinkRecord(topic, i, kafkaSchema.schema(), i, kafkaSchema, value, new Date().getTime());
                records.add(record);
            }
        } else {
            throw new KafkaException(String.format("Provided schemaVersion %d does not exist for topic %s ", schemaVersion, topic));
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

        option = Option.builder("c")
                .longOpt("total-batches")
                .desc("Number of batches to insert.")
                .argName("count")
                .hasArg(true)
                .type(PatternOptionBuilder.NUMBER_VALUE)
                .build();
        options.addOption(option);
        
        option = Option.builder("v")
                .longOpt("schema-version")
                .desc("Schema version of SchemaTest class.")
                .argName("version")
                .hasArg(true)
                .type(PatternOptionBuilder.NUMBER_VALUE)
                .build();
        options.addOption(option);
        
        option = Option.builder("s")
                .longOpt("schema-registry")
                .desc("Schema registry location")
                .argName("url:port")
                .hasArg(true)
                .build();
        options.addOption(option);
        
        // parse the command line arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse( options, args );

        if(line.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "SchemaTestSerializerDataPump [options] URL", options );
            System.exit(1);
        }

        if(line.hasOption('c')) {
            Long intObj = (Long)line.getOptionObject('c');
            System.out.println(String.format("Total batches: %d", intObj));
            SchemaTestSerializerDataPump.count = intObj.intValue();
        }

        if(line.hasOption('v')) {
            Long intObj = (Long)line.getOptionObject('v');
            System.out.println(String.format("Schema version: %d", intObj));
            SchemaTestSerializerDataPump.version = intObj.intValue();
        }

        if(line.hasOption('s')) {
            String url = (String)line.getOptionObject('s');
            System.out.println(String.format("Schema Registry: %s", url));
            SchemaTestSerializerDataPump.schemaRegistry = url;
        }

        // get URL
        if(line.getArgs().length == 0) {
            throw new Exception("Missing bootstrap URL");
        }
        SchemaTestSerializerDataPump.bootstrapUrl = line.getArgs()[0];
        System.out.println(String.format("URL: %s", SchemaTestSerializerDataPump.bootstrapUrl));

    }

    /**
     * Integration test helper class:
     * Data pump creating Kafka topic and autogenerating messages for it
     * This topic has four different Avro serialized schema versions for the same class name registered with Schema Registry.
     * Configuration allows to create messages of provided version. 
     * When version is unsupported or ommited in configuration properties, it creates a different topic with no-version serialized messages.  
     * 
     * During integration testing a properly configured KineticaSinkConnector is supposed to consume all messages into single table.
     *  
     * @param args     a list of command line arguments followed by Kafka Bootstrap URL (localhost:9092)
     */
    public static void main(String[] args) throws Exception{
        try {
            parseArgs(args);
        }
        catch(Exception ex) {
            System.out.println("PARAM ERROR: " + ex.getMessage());
            System.exit(1);
        }
    
        switch (SchemaTestSerializerDataPump.version) {
        case 1: case 2: case 3: case 4:            
            System.out.println("Producing SchemaTest records, schema version " + SchemaTestSerializerDataPump.version);
            produceAvroSerialized(SchemaRegistryUtils.getSchemaByVersion(version)); break;
        default:
            System.out.println(SchemaTestSerializerDataPump.version + " is not a valid version of SchemaTest, producing Employee records instead.");
            produceEmployeeSerialized(); break;
        }
    }
    
}
