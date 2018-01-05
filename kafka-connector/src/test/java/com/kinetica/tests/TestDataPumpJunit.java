package com.kinetica.tests;

import org.junit.Test;
import com.kinetica.kafka.tests.TestDataPump;

public class TestDataPumpJunit {

	@Test
    public void testHelp() throws Exception {
		String [] args = { "-h" };
		TestDataPump.main(args);
	}

	@Test
    public void testArgs() throws Exception {
		String [] args = { "-d", "3", "-n", "10", "http://localhost:9191" };
		TestDataPump.main(args);
	}

	@Test
    public void testNoArgs() throws Exception {
		String [] args = { "http://localhost:9191" };
		TestDataPump.main(args);
	}
}
