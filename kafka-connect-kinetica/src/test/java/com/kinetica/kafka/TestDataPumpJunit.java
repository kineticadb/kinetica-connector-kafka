package com.kinetica.kafka;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.gpudb.GPUdb;
import com.kinetica.kafka.TestDataPump;

/*
 * Tests helper class and determines Kinetica availablility before running integration tests
 */
public class TestDataPumpJunit {
	
    @After
    public void cleanup() throws Exception {
        GPUdb gpudb = TestUtils.getGPUdb();
        TestUtils.tableCleanUp(gpudb, "TEST.KafkaConnectorTest");
        TestUtils.tableCleanUp(gpudb, "TEST.KafkaConnectorTest2");
        gpudb = null;
    }

    @Test
    @Ignore // Code contains System.exit() 
    public void testHelp() throws Exception {
        String [] args = { "-h" };
        TestDataPump.main(args);
    }

    @Test
    public void testArgs() throws Exception {
        String [] args = { "-d", "0", "-n", "2", "-t", "5", "-c", "config/quickstart-kinetica-sink.properties" };
        TestDataPump.main(args);
    }

    @Test
    @Ignore // Code contains System.exit() 
    public void testNoArgs() throws Exception {
        String [] args = { "-d", "0", "http://localhost:9191" };
        TestDataPump.main(args);
    }
}
