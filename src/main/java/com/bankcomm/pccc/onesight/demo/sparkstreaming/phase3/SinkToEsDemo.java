package com.bankcomm.pccc.onesight.demo.sparkstreaming.phase3;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SinkToEsDemo
{
    private static final String bootstrapServers = "hadoop2:9092,hadoop3:9092,hadoop4:9092";
    private static final String input_topic = "fin";
    private static final String es_nodes = "hadoop2:9200,hadoop3:9200,hadoop4:9200";
    
    private static void runSparkStreamingDemo() throws InterruptedException {
        JavaStreamingContext sc = initSparkStreamingContext4Es();
        
        JavaInputDStream<ConsumerRecord<String, String>> lines = readStreamFromKafka(sc);
        manageOffsets(lines);
        
        JavaDStream<String> stream = lines.map(x -> x.value());
        sendToEs(stream);
        
        startAndWait(sc);
    }

    private static void startAndWait(JavaStreamingContext sc) throws InterruptedException {
        sc.start();
        sc.awaitTermination();
    }

    private static void manageOffsets(JavaInputDStream<ConsumerRecord<String, String>> lines) {
        lines.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) lines.inputDStream()).commitAsync(offsetRanges);
        });
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> readStreamFromKafka(JavaStreamingContext sc) {
        return KafkaUtils.createDirectStream(
                sc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(initTopics(), initKafkaParams())
        );
    }

    private static Set<String> initTopics() {
        return Sets.newHashSet(input_topic);
    }

    private static Map<String, Object> initKafkaParams() {
        Map<String, Object> kafkaParams = Maps.newHashMap();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }
    
    private static JavaStreamingContext initSparkStreamingContext4Es() {
        SparkConf sparkConf = new SparkConf()
                            .setMaster("local[2]")
                            .setAppName("KafkaDirectWordCount")
                            .set("es.nodes", es_nodes)
                            .set("es.index.auto.create","true");
        
        return new JavaStreamingContext(sparkConf, Durations.seconds(2));
    }
    
    private static void sendToEs(JavaDStream<String> stream)
    {
        JavaEsSparkStreaming.saveJsonToEs(stream, "spark/docs");  
    }
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
