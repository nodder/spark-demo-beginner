package com.bankcomm.pccc.onesight.demo.sparkstreaming;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
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

import com.bankcomm.pccc.onesight.demo.spark.framework.KafkaProducerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import scala.Tuple2;

/**
 * 7. KafkaToKafkaDirectAccWordCount_newVersion
 * 对比KafkaToKafkaDirectAccWordCount_oldVersion，使用了新的api，不会有deprecated问题
 * next：
 * @author A175703
 */
public class KafkaToKafkaDirectAccWordCount_newVersion {
    private static final String jsonTemplate = "[{\"metric\":\"#metricName#\",\"tags\":{\"cdd\":\"mms\",\"test\":\"xx01\"},\"timestamp\":#timestamp#,\"value\":\"#value#\"}]";
    
    private static void runSparkStreamingDemo() throws InterruptedException {
        JavaStreamingContext sc = initSparkStreamingContext();
        
        sc.checkpoint("D:\\program_data\\checkpoint");
        
        Broadcast<String> bcOutputKafkaBrokerList = sc.sparkContext().broadcast("localhost:9092");//大数据平台kafka：182.180.115.73:9092,182.180.115.74:9092,182.180.115.75:9092
        Broadcast<String> bcOutputTopic = sc.sparkContext().broadcast("zb_to_tsdb_test");//大数据平台kafka：zb_to_tsdb
        Broadcast<String> bcJsonTemplate = sc.sparkContext().broadcast(jsonTemplate);
        
        JavaInputDStream<ConsumerRecord<String, String>> lines = readStreamFromKafka(sc);
        manageOffsets(lines);
        
        /**
         * 对于updateStateByKey方法的观察：
         * 在每个时间间隔到后，如果期间有输入，或者第二个参数（optional）非空，或者第一次执行，都会调用updateStateByKey方法
         * 如果时间间隔内没有输入，则values是空列表，optionCount存的是checkpoint中的旧数据
         */
        JavaPairDStream<String, Integer> counts = lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .updateStateByKey((values, optionCount) -> {
                    System.out.println(values.size());
                    System.out.println(optionCount);
                    int currCount = optionCount.or(0);
                    
                    if(currCount == 5)//到5以后从0开始。调试用，无实际用途。
                    {
                        return Optional.empty();
                    }
                       
                    return Optional.of(values.stream().reduce(currCount, (v1, v2) -> v1 + v2));
                });
        
        /**
         * 对于foreachPartition，无论时间间隔内没有输入，时间间隔到后，此方法都会被执行N次（N为一个rdd对应的partition个数）
         * 对于wordAndCountPairIt，是否执行与updateStateByKey规则一致
         */
        counts.foreachRDD(rdd ->  rdd.foreachPartition(             
                wordAndCountPairIt -> sendToKafka(assembleMessageList(bcJsonTemplate.getValue(), wordAndCountPairIt), bcOutputKafkaBrokerList.getValue(), bcOutputTopic.getValue())
                ));
        
        startAndWait(sc);
    }

    private static List<String> assembleMessageList(String value, Iterator<Tuple2<String, Integer>> wordAndCountPairIt) {
        List<String> messageList = Lists.newArrayList();
        
        while (wordAndCountPairIt.hasNext()) {
            Tuple2<String, Integer> pair = wordAndCountPairIt.next();
            System.out.println(pair + "::::" + pair._1 + "|" + pair._2);
            String json = jsonTemplate.replace("#metricName#", pair._1)
                                      .replace("#value#", pair._2 + "")
                                      .replace("#timestamp#", new Date().getTime()+ "");
          messageList.add(json);
        }
        
        return messageList;
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
    
    private static void sendToKafka(List<String> messageList, String bootstrapServers, String topic) {
        Producer<String, String> producer = KafkaProducerFactory.createKafkaProducer(bootstrapServers);
        
        for(String message : messageList)
        {
            ProducerRecord<String, String> produceRecord = new ProducerRecord<>(topic, message);

            producer.send(produceRecord);
            System.out.println("sent ok");
        }
    }
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
