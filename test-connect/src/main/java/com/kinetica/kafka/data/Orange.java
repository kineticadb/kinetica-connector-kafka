package com.kinetica.kafka.data;

import com.kinetica.kafka.data.utils.RandomGenerator;

/**
 * Helper class - Orange POJO with embedded schema
 * @author nataliya tairbekov
 *
 */
public class Orange {
    
    public int id;
    public String sort;
    public int diameter;
    public float weight;
    private static String schema = "{\"type\":\"record\",\"name\":\"Orange\",\"namespace\":\"com.kinetica.kafka.data\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"sort\",\"type\":\"string\"},{\"name\":\"diameter\",\"type\":[\"null\",\"int\"]},{\"name\":\"weight\",\"type\":[\"null\",\"float\"]}]}";
    
    public static Orange generateOrange(int id) {
        Orange obj = new Orange();
        obj.id = id;
        obj.sort = RandomGenerator.getRandomString(10);
        obj.diameter = RandomGenerator.random.nextInt(10);
        obj.weight = RandomGenerator.random.nextFloat()*100;
        return obj;
    }    
    
    public static String getSchema() {
        return schema;
    }
}

