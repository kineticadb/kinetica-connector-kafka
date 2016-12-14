package com.gpudb.kafka.tests;

import com.gpudb.ColumnProperty;
import com.gpudb.RecordObject;
import java.util.Random;

/**
 *
 */
public class TweetRecord extends RecordObject{
    @RecordObject.Column(order = 0, properties = { ColumnProperty.DATA })
    public float x;

    @RecordObject.Column(order = 1, properties = { ColumnProperty.DATA })
    public float y;

    @RecordObject.Column(order = 2, properties = { ColumnProperty.DATA })
    public long timestamp;

    @RecordObject.Column(order = 3, properties = { ColumnProperty.DATA })
    public String TEXT;

    @RecordObject.Column(order = 4, properties = { ColumnProperty.DATA })
    public String AUTHOR;

    @RecordObject.Column(order = 5, properties = { ColumnProperty.DATA })
    public String URL;
    
    public String getRandomString(int length){
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
        record.timestamp    = rnd.nextLong();
        record.TEXT         = record.getRandomString(144);
        record.AUTHOR       = record.getRandomString(32);
        record.URL          = record.getRandomString(256);
        return(record);
    }
}
