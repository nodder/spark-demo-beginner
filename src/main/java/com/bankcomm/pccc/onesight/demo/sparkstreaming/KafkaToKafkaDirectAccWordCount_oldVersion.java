package com.bankcomm.pccc.onesight.demo.sparkstreaming;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import com.bankcomm.pccc.onesight.demo.spark.framework.KafkaProducer_oldVersion;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.producer.KeyedMessage;
import scala.Tuple2;

/**
 * 7. KafkaToKafkaDirectAccWordCount_oldVersion
 * 对比KafkaDirectAccWordCount，输出从打印改为输出到kakfa。
 * 但发送到kafka使用的是deprecated的方式。新方式可以参考KafkaToKafkaDirectAccWordCount_newVersion。
 * 【注意】：要正常使用需要注释掉pom文件 kafka-clients和lz4两个依赖才能运行成功。
 * next：
 * @author A175703
 */
@SuppressWarnings("deprecation")
public class KafkaToKafkaDirectAccWordCount_oldVersion {
    private static final String jsonTemplate = "[{\"metric\":\"#metricName#\",\"tags\":{\"cdd\":\"mms\",\"test\":\"xx01\"},\"timestamp\":#timestamp#,\"value\":\"#value#\"}]";
    
    private static void runSparkStreamingDemo() throws InterruptedException {
        JavaStreamingContext sc = initSparkStreamingContext();
        
        sc.checkpoint("D:\\program_data\\checkpoint");
        
        Broadcast<String> bcOutputKafkaBrokerList = sc.sparkContext().broadcast("localhost:9092");
        Broadcast<String> bcTopic = sc.sparkContext().broadcast("zb_to_tsdb_test");
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
                wordAndCountPairIt -> sendToKafka_oldVersion(bcOutputKafkaBrokerList.getValue(), 
                                                  assmebleMessageList(bcTopic.getValue(), bcJsonTemplate.getValue(), wordAndCountPairIt))
                
                ));
        
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
    
    private static void sendToKafka_oldVersion(String kafkaBrokerList, List<KeyedMessage<String, String>> messageList) {
        KafkaProducer_oldVersion kafkaProducer = KafkaProducer_oldVersion.getInstance(kafkaBrokerList);
        kafkaProducer.send(messageList);
    }
    
    private static List<KeyedMessage<String, String>> assmebleMessageList(String topic, String jsonTemplate, Iterator<Tuple2<String, Integer>> wordAndCountPairIt) {
        List<KeyedMessage<String, String>> messageList = Lists.newArrayList();
        while (wordAndCountPairIt.hasNext()) {
            Tuple2<String, Integer> pair = wordAndCountPairIt.next();
            System.out.println(pair + "::::" + pair._1 + "|" + pair._2);
            String json = jsonTemplate.replace("#metricName#", pair._1)
                                      .replace("#value#", pair._2 + "")
                                      .replace("#timestamp#", new Date().getTime()+ "");
          messageList.add(new KeyedMessage<String, String>(topic, json));
        }
        return messageList;
    }

    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
