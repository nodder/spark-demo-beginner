package com.bankcomm.pccc.onesight.demo.sparkstreaming;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import scala.Tuple2;

/**
 * 6. KafkaToKafkaDirectAccWordCount
 * 从kafka输入的实时统计数字，不同batch之间支持累加。
 * next：KafkaToKafkaDirectAccWordCount
 * @author A175703
 * 
 * 对KafkaDirectWordCount进行了重构，增加checkpoint累加所有分片结果
 * @author A175703
 * 测试：中途保留kafka，重启程序，计数从头开始；
 *       中途重开kafka，计数继续
 */
public class KafkaDirectAccWordCount {
    private static void runSparkStreamingDemo() throws InterruptedException {
        JavaStreamingContext sc = initSparkStreamingContext();
        
        /**
         * 启动checkpoint机制
         */
        sc.checkpoint("D:\\program_data\\checkpoint");
        
        JavaInputDStream<ConsumerRecord<String, String>> lines = readStreamFromKafka(sc);
        manageOffsets(lines);
        
        /**
         * updateStateByKey机制，可以让我们为每个key维护一份state，并持续不断地更新该state。
         * 对于每个batch，Spark都会为每个之前已经存在的key去应用一次state更新函数，无论这个key在batch中是否有新的数据。
         * 如果state更新返回none，那么key对应的state就会被删除。
         * 
         * 对于updateStateByKey操作，必须开启CheckPoint机制。
         */
        JavaPairDStream<String, Integer> counts = lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .updateStateByKey((values, optionCount) -> {
                    int currCount = optionCount.or(0);
                    return Optional.of(values.stream().reduce(currCount, (v1, v2) -> v1 + v2));
                });

        counts.print();
        
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
        return Sets.newHashSet("sparktest");
    }

    private static Map<String, Object> initKafkaParams() {
        Map<String, Object> kafkaParams = Maps.newHashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    private static JavaStreamingContext initSparkStreamingContext() {
        SparkConf sparkConf = new SparkConf()
                            .setMaster("local[2]")
                            .setAppName("KafkaDirectWordCount");
        
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        return sc;
    }
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
