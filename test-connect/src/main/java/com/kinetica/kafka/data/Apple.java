package com.kinetica.kafka.data;

import com.kinetica.kafka.data.utils.RandomGenerator;
/**
 * Helper class - Apple POJO with embedded schema
 * @author nataliya tairbekov
 *
 */
public class Apple 
{
    public int id;
    public String sort;
    public int size;
    public float weight;
    private static String schema = "{\"type\":\"record\",\"name\":\"Apple\",\"namespace\":\"com.kinetica.kafka.data\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"sort\",\"type\":\"string\"},{\"name\":\"size\",\"type\":[\"null\",\"int\"]},{\"name\":\"weight\",\"type\":[\"null\",\"float\"]}]}";

    public static Apple generateApple(int id) {
        Apple obj = new Apple();
        obj.id = id;
        obj.sort = RandomGenerator.getRandomString(10);
        obj.size = RandomGenerator.random.nextInt(100);
        obj.weight = RandomGenerator.random.nextFloat()*100;
        return (obj);
    }
    
    public static String getSchema() {
        return schema;
    }
}