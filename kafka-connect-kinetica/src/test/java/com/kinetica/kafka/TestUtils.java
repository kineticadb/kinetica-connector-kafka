package com.kinetica.kafka;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.errors.ConnectException;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;

public class TestUtils {
    private final static String DEFAULT_COLLECTION = "ki_home";
    private final static String CFG_PATH = "config/quickstart-kinetica-sink.properties";

    public static GPUdb getGPUdb() throws Exception {
    	Properties props = new Properties();
        try (Reader propsReader = new FileReader(CFG_PATH)) {
            props.load(propsReader);
        }
        
        Map<String, String> baseConfig = new HashMap<String, String>((Map) props);  
        
        try {
            GPUdb gpudb = new GPUdb(baseConfig.get(KineticaSinkConnectorConfig.PARAM_URL), 
                    new GPUdb.Options()
                    .setUsername( baseConfig.get(KineticaSinkConnectorConfig.PARAM_USERNAME))
                    .setPassword( baseConfig.get(KineticaSinkConnectorConfig.PARAM_PASSWORD))
                    .setTimeout(Integer.parseInt( baseConfig.get(KineticaSinkConnectorConfig.PARAM_TIMEOUT))));
            return gpudb;
        }
        catch (GPUdbException ex) {
            ConnectException cex = new ConnectException("Unable to connect to Kinetica at: " + baseConfig.get(KineticaSinkConnectorConfig.PARAM_URL), ex);
            throw cex;
        }
    }

    /**
     * Helper function used to cleanup a list of KINETICA tables (either default tablenames, or prefixed with PREFIX)
     * @param prefixed    Flag notifying that table name was manipulated
     * @throws Exception
     */    
    public static void tableCleanUp(GPUdb gpudb, String tableName) throws Exception {
    	
        String schemaName = null;
        // Kinetica table cleanup before running data ingest
        if(gpudb.hasTable(tableName, null).getTableExists()) {
            if (tableName.contains(".")) {
                // Extract table schema to be dropped separately
                schemaName = tableName.split("[.]")[0];
            }
            gpudb.clearTable(tableName, null, null);
        }
        // Do not attempt to remove default collection
    	if (schemaName != null && !DEFAULT_COLLECTION.equalsIgnoreCase(schemaName) 
    			&& gpudb.hasSchema(schemaName, null).getSchemaExists()) {
        	try {
            	com.gpudb.protocol.ShowSchemaResponse resp = gpudb.showSchema(schemaName, null);
            	// Schema is dropped only if there are no tables within
            	if(resp.getSchemaTables() != null && resp.getSchemaTables().size() == 1 
                		&& resp.getSchemaTables().get(0).size() == 0) {
                	gpudb.dropSchema(schemaName, null);
                }
            } catch (GPUdbException e)  {
                return;
            }
    	}
    }
    public static void tableCleanUp(String tableName) throws Exception {
    	GPUdb gpudb = getGPUdb();
    	tableCleanUp(gpudb, tableName);
    	gpudb = null;
    }
}
