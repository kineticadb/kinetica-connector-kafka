package com.kinetica.kafka.data.utils;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * Helper class for random value generation to be used when autopopulating POJO properties
 * @author nataliya
 *
 */
public class RandomGenerator {
    
    public static Random random = new Random();
    
    public static String getRandomString(int length){
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ";
        String str = "";
       
        for( int i = 0; i < length; i++ ){
            str += chars.charAt(random.nextInt(chars.length()));
        }
        return(str);
    }

    public static  Long getRandomDate() {
        Calendar calendar = Calendar.getInstance();
        Long endTime = calendar.getTimeInMillis();
        calendar.add(Calendar.YEAR,  -1);
        Long beginTime = calendar.getTimeInMillis();

        long diff = endTime - beginTime + 1;
        long date = beginTime + (long)(random.nextFloat() * diff);
        return date;
    }
    
    public static String getFormattedRandomDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
        Calendar calendar = Calendar.getInstance();
        Long endTime = calendar.getTimeInMillis();
        calendar.add(Calendar.YEAR,  -1);
        Long beginTime = calendar.getTimeInMillis();

        long diff = endTime - beginTime + 1;
        long date = beginTime + (long)(random.nextFloat() * diff);
        return df.format(date);
    }
    
    public static  Object randomValueByType (Schema schema) {
        Schema.Type type = schema.getType();
        switch (type) {
        case STRING:  return getRandomString(24);
        case BYTES:   return ByteBuffer.wrap(getRandomString(48).getBytes());
        case INT:     return random.nextInt(1000);
        case LONG:    return random.nextLong();
        case FLOAT:   return random.nextFloat();
        case DOUBLE:  return random.nextDouble();
        case UNION: {
            for (Schema subType : schema.getTypes()) {
                if (subType.getType() != Schema.Type.NULL) {
                      return randomValueByType(subType);
                }
            }
        }
        case NULL:    
        default:      return null;
        }
    }

    public static GenericRecord populateGenericRecord (Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Schema.Field field: schema.getFields()) {
            if (field.name().equalsIgnoreCase("timestamp")) {
                builder.set(field.name(), getRandomDate());
            } else {
                builder.set(field.name(), randomValueByType(field.schema()));
            }
        }
        
                
        return builder.build();
    }

}
