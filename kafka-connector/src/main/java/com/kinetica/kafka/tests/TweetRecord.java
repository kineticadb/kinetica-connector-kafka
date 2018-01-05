package com.kinetica.kafka.tests;

import com.gpudb.ColumnProperty;
import com.gpudb.RecordObject;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 *
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
        record.test_int 	    = rnd.nextInt();
        record.test_long    = rnd.nextLong();
        record.test_double 	= rnd.nextDouble();
        record.test_bytes   = ByteBuffer.wrap(getRandomString(100).getBytes());
        return(record);
    }
}
