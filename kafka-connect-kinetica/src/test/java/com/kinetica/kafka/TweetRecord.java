package com.kinetica.kafka;

import com.gpudb.ColumnProperty;
import com.gpudb.RecordObject;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;

/**
 * POJO providing fields of various column data types
 */
public class TweetRecord extends RecordObject{
    @RecordObject.Column(order = 0, properties = { ColumnProperty.DATA })
    public float x;

    @RecordObject.Column(order = 1, properties = { ColumnProperty.DATA })
    public float y;

    @RecordObject.Column(order = 2, properties = { ColumnProperty.DATA, ColumnProperty.TIMESTAMP })
    public long timestamp;

    @RecordObject.Column(order = 3, properties = { ColumnProperty.DATA })
    public String TEXT;

    @RecordObject.Column(order = 4, properties = { ColumnProperty.DATA })
    public String AUTHOR;

    @RecordObject.Column(order = 5, properties = { ColumnProperty.DATA })
    public String URL;

    @RecordObject.Column(order = 6, properties = { ColumnProperty.DATA })
    public int test_int;

    @RecordObject.Column(order = 7, properties = { ColumnProperty.DATA })
    public long test_long;

    @RecordObject.Column(order = 8, properties = { ColumnProperty.DATA })
    public double test_double;

    @RecordObject.Column(order = 9, properties = { ColumnProperty.STORE_ONLY })
    public ByteBuffer test_bytes;

    @RecordObject.Column(order = 10, properties = {ColumnProperty.STORE_ONLY, ColumnProperty.CHAR16, ColumnProperty.NULLABLE})
    public String test_schema_reg;
    
    public static String getRandomString(int length){
        String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ";
        String str = "";
        Random rnd = new Random();
        for( int i = 0; i < length; i++ ){
            str += chars.charAt(rnd.nextInt(chars.length()));
        }
        return(str);
    }

    public static TweetRecord generateRandomRecord(){
        TweetRecord record = new TweetRecord();
        Random rnd          = new Random();
        record.x            = rnd.nextFloat()*360 - 180;
        record.y            = rnd.nextFloat()*180 - 90;
        record.timestamp    = System.currentTimeMillis();
        record.TEXT         = getRandomString(144);
        record.AUTHOR       = getRandomString(32);
        record.URL          = getRandomString(256);
        record.test_int     = rnd.nextInt();
        record.test_long    = rnd.nextLong();
        record.test_double  = rnd.nextDouble();
        record.test_bytes   = ByteBuffer.wrap(getRandomString(100).getBytes());
        record.test_schema_reg = getRandomString(16);
        return(record);
    }
    
    public static HashMap<String, Object> generateHashMap() {
        TweetRecord record = generateRandomRecord();
        HashMap<String, Object> hm = new HashMap<String, Object>();
        hm.put("x", record.x);
        hm.put("y", record.y);
        hm.put("timestamp", record.timestamp);
        hm.put("TEXT", record.TEXT);
        hm.put("AUTHOR", record.AUTHOR);
        hm.put("URL", record.URL);
        hm.put("test_int", record.test_int);
        hm.put("test_long", record.test_long);
        hm.put("test_double", record.test_double);
        hm.put("test_bytes", record.test_bytes);
        hm.put("test_schema_reg", record.test_schema_reg);
        
        record = null;
        return hm;
    }
}
