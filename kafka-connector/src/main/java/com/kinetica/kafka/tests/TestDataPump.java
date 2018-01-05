package com.kinetica.kafka.tests;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PatternOptionBuilder;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.gpudb.protocol.CreateTableRequest;

/**
 * Pump data into Kinetica to be used as a Kafka connector source
 * @author dkatz
 */
public class TestDataPump {

	private static String COLLECTION_NAME = "TEST";
	private static String TWEET_SOURCE_TABLE = "KafkaConnectorTest";
	private static String TWEET_SOURCE_TABLE2 = "KafkaConnectorTest2";

	// command line params
	private static String gpudbUrl = null;
	private static int batchSize = 10;
	private static int delaySeconds = 3;


	public static void main(String[] args) throws Exception {

		try {
			parseArgs(args);
		}
		catch(Exception ex) {
			System.out.println("PARAM ERROR: " + ex.getMessage());
			System.exit(1);
		}

		GPUdb gpudb = new GPUdb(gpudbUrl);

		// Create schema type and table
		createTable(gpudb, TWEET_SOURCE_TABLE);
		createTable(gpudb, TWEET_SOURCE_TABLE2);

		while(true) {
			//Create records
			insertRecords(gpudb, TWEET_SOURCE_TABLE);
			insertRecords(gpudb, TWEET_SOURCE_TABLE2);
			Thread.sleep(delaySeconds*1000);
		}
	}

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
				.desc("Number of records")
				.argName("count")
				.type(PatternOptionBuilder.NUMBER_VALUE)
				.hasArg()
				.build();
		options.addOption(option);

		option = Option.builder("d")
				.longOpt("delay-time")
				.desc("Seconds between batches.")
				.argName("seconds")
				.hasArg(true)
				.type(PatternOptionBuilder.NUMBER_VALUE)
				.build();
		options.addOption(option);

		// parse the command line arguments
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse( options, args );

		if(line.hasOption('h')) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "TestDataPump [options] URL", options );
			System.exit(1);
		}

		if(line.hasOption('d')) {
			Long intObj = (Long)line.getOptionObject('d');
			System.out.println(String.format("Delay seconds: %d", intObj));
			TestDataPump.delaySeconds = intObj.intValue();
		}

		if(line.hasOption('n')) {
			Long intObj = (Long)line.getOptionObject('n');
			System.out.println(String.format("Batch count: %d", intObj));
			TestDataPump.batchSize = intObj.intValue();
		}

		// get URL
		if(line.getArgs().length == 0) {
			throw new Exception("Missing GPUdb URL");
		}
		TestDataPump.gpudbUrl = line.getArgs()[0];
		System.out.println(String.format("URL: %s", TestDataPump.gpudbUrl));
	}

	private static void insertRecords(GPUdb gpudb, String tableName) throws Exception {
		//Create records
		ArrayList<TweetRecord> records = new ArrayList<>();
		for(int i=0 ; i < batchSize; i++){
			records.add(TweetRecord.generateRandomRecord());
		}

		//Insert records
		BulkInserter<TweetRecord> bulkInserter = new BulkInserter<>(gpudb, tableName, records.get(0).getType(), 1, null);
		bulkInserter.insert(records);
		bulkInserter.flush();
		records.clear();
		System.out.println(String.format("Inserted %d records into %s.", batchSize, tableName));
	}

	private static void createTable(GPUdb gpudb, String tableName) throws GPUdbException
	{
		if(gpudb.hasTable(tableName, null).getTableExists()) {
			System.out.println("Table exists: " + tableName);
			return;
		}

		System.out.println("Crating table: " + tableName);
		String typeId = RecordObject.createType(TweetRecord.class, gpudb);
		gpudb.addKnownType(typeId, TweetRecord.class);
		gpudb.createTable(tableName, typeId,
				GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, COLLECTION_NAME));
	}
}
