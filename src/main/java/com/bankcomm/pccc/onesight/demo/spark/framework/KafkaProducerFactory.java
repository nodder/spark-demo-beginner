package com.bankcomm.pccc.onesight.demo.spark.framework;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class KafkaProducerFactory {
    private static Producer<String, String> instance;
    
    private KafkaProducerFactory() {};
    
    public static synchronized Producer<String, String> createKafkaProducer(String bootstrapServers) {
        if(instance == null)
        {
            instance = new KafkaProducer<String, String>(assembleProperties(bootstrapServers));
        }
        
        return instance;
    }

    private static Properties assembleProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", bootstrapServers);
        
        return props;
    }
}
