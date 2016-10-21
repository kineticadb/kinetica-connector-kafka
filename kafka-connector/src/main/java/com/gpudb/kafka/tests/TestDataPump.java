package com.gpudb.kafka.tests;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.gpudb.BulkInserter;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pump data into GPUdb to be used as a Kafka connector source
 * @author dkatz
 */
public class TestDataPump {
    
    public static void main(String[] args){
        if(args.length==0){
            System.out.printf("Missing GPUdb URL\n");
            System.exit(1);
        }
        String gpudbURL = args[0];
        GPUdb gpudb;
        try {
            gpudb = new GPUdb(gpudbURL);
        } catch (GPUdbException ex) {
            Logger.getLogger(TestDataPump.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        // Create schema type and table
        String tableName = "KafkaConnectorTest";
        try{
            String typeId = RecordObject.createType(TweetRecord.class, gpudb);
            gpudb.addKnownType(typeId, TweetRecord.class);
            gpudb.createTable(tableName, typeId, null);
        }
        catch (GPUdbException e){
            if (e.getMessage().contains("already exists")){
                System.out.println("Table " + tableName + " already exists");
            }
        } 
        
        while(true){
            //Create records
            ArrayList<TweetRecord> records = new ArrayList<>();
            for(int i=0;i<1000;i++){
                records.add(TweetRecord.generateRandomRecord());
            }

            //Insert records
            try {
                BulkInserter<TweetRecord> bulkInserter = new BulkInserter<>(gpudb,tableName,records.get(0).getType(),1,null );
                bulkInserter.insert(records);
                bulkInserter.flush();
                records.clear();
            } catch (GPUdbException ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
}
