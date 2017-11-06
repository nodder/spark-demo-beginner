package com.bankcomm.pccc.onesight.demo.spark.framework;


import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Created by peterliao on 17-8-17.
 */
public class KafkaProducer_oldVersion implements Serializable {

    public static final String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
    public static final String SERIALIZER_CLASS_KEY = "serializer.class";
    public static final String SERIALIZER_CLASS_VALUE = "kafka.serializer.StringEncoder";

    private static KafkaProducer_oldVersion instance = null;

    private Producer producer;

    private KafkaProducer_oldVersion(String brokerList) {

        Preconditions.checkArgument(StringUtils.isNotBlank(brokerList), "kafka brokerList is blank...");

        // set properties
        Properties properties = new Properties();
        properties.put(METADATA_BROKER_LIST_KEY, brokerList);
        properties.put(SERIALIZER_CLASS_KEY, SERIALIZER_CLASS_VALUE);
        properties.put("kafka.message.CompressionCodec", "1");
        properties.put("client.id", "streaming-kafka-output");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.producer = new Producer(producerConfig);
    }

    public static synchronized KafkaProducer_oldVersion getInstance(String brokerList) {
        if (instance == null) {
            instance = new KafkaProducer_oldVersion(brokerList);
        }
        return instance;
    }

    // 单条发送
    public void send(KeyedMessage<String, String> keyedMessage) {
        producer.send(keyedMessage);
    }

    // 批量发送
    public void send(List<KeyedMessage<String, String>> keyedMessageList) {
        producer.send(keyedMessageList);
    }

    public void shutdown() {
        producer.close();
    }


}