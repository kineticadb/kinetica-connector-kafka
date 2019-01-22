package com.kinetica.kafka.data;

import com.kinetica.kafka.data.utils.RandomGenerator;
/**
 * Helper class - Banana POJO with embedded schema
 * @author nataliya tairbekov
 *
 */
public class Banana {
    
    public int id;
    public String sort;
    public float length;
    public float weight;
    private static String schema = "{\"type\":\"record\",\"name\":\"Banana\",\"namespace\":\"com.kinetica.kafka.data\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"sort\",\"type\":\"string\"},{\"name\":\"length\",\"type\":[\"null\",\"float\"]},{\"name\":\"weight\",\"type\":[\"null\",\"float\"]}]}";
    
    public static Banana generateBanana(int id) {
        Banana obj = new Banana();
        obj.id = id;
        obj.sort = RandomGenerator.getRandomString(10);
        obj.length = RandomGenerator.random.nextFloat()*10;
        obj.weight = RandomGenerator.random.nextFloat()*100;
        return obj;
    }
    
    public static String getSchema() {
        return schema;
    }
}

