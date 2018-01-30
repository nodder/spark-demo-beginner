package com.bankcomm.pccc.onesight.demo.sparkstreaming.phase2;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bankcomm.pccc.onesight.demo.spark.framework.KafkaProducerFactory;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import scala.Tuple2;

public class ReduceDemo
{
    private static void runSparkStreamingDemo() throws InterruptedException {
        JavaStreamingContext sc = initSparkStreamingContext();
        
        JavaInputDStream<ConsumerRecord<String, String>> lines = readStreamFromKafka(sc);
        manageOffsets(lines);
        
        
//        findMax(lines);
//       findMax2(lines);
//        findMax3(lines);
//        findMax4(lines);
        
        findMax5(lines);
        
//        result.foreachRDD(rdd -> rdd.foreachPartition(
//            x -> System.out.println(x)
//                        ));
        
        
//        JavaDStream<String> x = lines.map(line ->line.value());
//        x.print();
                        
//                        .flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
//                .reduceByKey((x, y) -> Math.max(x, y));
        
//      counts.foreachRDD(rdd ->  rdd.foreachPartition(             
//              wordAndMaxCountPairIt -> wordAndMaxCountPairIt.forEachRemaining(System.out::println)));
//        
//                
//                .updateStateByKey((values, optionCount) -> {
//                    System.out.println(values.size());
//                    System.out.println(optionCount);
//                    int currCount = optionCount.or(0);
//                    
//                    if(currCount == 5)//到5以后从0开始。调试用，无实际用途。
//                    {
//                        return Optional.empty();
//                    }
//                       
//                    return Optional.of(values.stream().reduce(currCount, (v1, v2) -> v1 + v2));
//                });
        
        startAndWait(sc);
    }


    /**
     * {"time":1000, "key1": "aa", "data": [{"key2":3, "other":"aaa"}, {"key2":2, "other":"aaa"}]}
{"time":1000, "key1": "aa", "data": [{"key2":2, "other":"aaa"}, {"key2":6, "other":"aaa"}]}
{"time":1000, "key1": "aa", "data": [{"key2":4, "other":"aaa"}, {"key2":3, "other":"aaa"}]}
{"time":1001, "key1": "aa", "data": [{"key2":2, "other":"aaa"}, {"key2":2, "other":"aaa"}]}
{"time":1001, "key1": "aa", "data": [{"key2":2, "other":"aaa"}, {"key2":7, "other":"aaa"}]}
     * @param lines
     */
 
    /** (1001,{"key1":"aa","data":[{"key2":2,"other":"aaa"},{"key2":7,"other":"aaa"}],"time":1001})*/
    private static void findMax5(JavaInputDStream<ConsumerRecord<String, String>> lines)
    {
        lines.map(line -> parseObject(line.value()))
        .filter(Optional::isPresent)
        .map(opJsonObj -> opJsonObj.get())
        .filter(jsonObj -> jsonObj.containsKey("time"))//TODO del
        .mapToPair(jsonObj -> new Tuple2<Integer, JSONObject>(jsonObj.getInteger("time"), jsonObj))//JavaPairDStream<Integer, JSONObject> 
        .groupByKey()//JavaPairDStream<Integer, Iterable<JSONObject>> 
        .mapToPair(time_jsonIt_tuple -> new Tuple2<>(time_jsonIt_tuple._1, findMax4ByKey(time_jsonIt_tuple._2, "data", "key2")))   //JavaPairDStream<Integer, JSONObject> 
        .reduce((time_jsonIt_tuple1, time_jsonIt_tuple2) -> time_jsonIt_tuple1._1 > time_jsonIt_tuple2._1 ? time_jsonIt_tuple1 : time_jsonIt_tuple2)
        .print();
    }


    /**
     * {"time":1000, "key1": "aa", "data": [{"key2":3, "other":"aaa"}, {"key2":2, "other":"aaa"}]}
{"time":1000, "key1": "aa", "data": [{"key2":2, "other":"aaa"}, {"key2":6, "other":"aaa"}]}
{"time":1000, "key1": "aa", "data": [{"key2":4, "other":"aaa"}, {"key2":3, "other":"aaa"}]}
{"time":1001, "key1": "aa", "data": [{"key2":2, "other":"aaa"}, {"key2":2, "other":"aaa"}]}
{"time":1001, "key1": "aa", "data": [{"key2":2, "other":"aaa"}, {"key2":7, "other":"aaa"}]}
     * @param lines
     */
    
    /*(1000,{"key1":"aa","data":[{"key2":2,"other":"aaa"},{"key2":6,"other":"aaa"}],"time":1000})
    (1001,{"key1":"aa","data":[{"key2":2,"other":"aaa"},{"key2":7,"other":"aaa"}],"time":1001})*/
    static void findMax4(JavaInputDStream<ConsumerRecord<String, String>> lines)
    {
        lines.map(line -> parseObject(line.value()))
        .filter(Optional::isPresent)
        .map(opJsonObj -> opJsonObj.get())
        .filter(jsonObj -> jsonObj.containsKey("time"))//TODO del
        .mapToPair(jsonObj -> new Tuple2<Integer, JSONObject>(jsonObj.getInteger("time"), jsonObj))//JavaPairDStream<Integer, JSONObject> 
        .groupByKey()//JavaPairDStream<Integer, Iterable<JSONObject>> 
        .mapToPair(time_jsonIt_tuple -> new Tuple2<>(time_jsonIt_tuple._1, findMax4ByKey(time_jsonIt_tuple._2, "data", "key2")))   //JavaPairDStream<Integer, JSONObject> 
        .print();
    }
    

    /*
     * {"time":1000, "key1": 100, "key2": 1000}
        {"time":1000, "key1": 99, "key2": 1009}
        {"time":1000, "key1": 108, "key2": 1000}
        {"time":1000, "key1": 108, "key2": 1007}
        {"time":1000, "key1": 108, "key2": 1005}
        {"time":1001, "key1": 100, "key2": 1005}
        {"time":1001, "key1": 109, "key2": 1000}
        {"time":1001, "key1": 101, "key2": 1008}
        {"time":1002, "key1": 107, "key2": 1009}
     */
  /*   输出
     (1002,{"key1":107,"key2":1009,"time":1002})
     (1000,{"key1":100,"key2":1000,"time":1000})
     (1001,{"key1":100,"key2":1005,"time":1001})*/
    static void findMax3(JavaInputDStream<ConsumerRecord<String, String>> lines)
    {
        lines.map(line -> parseObject(line.value()))
            .filter(Optional::isPresent)
            .map(opJsonObj -> opJsonObj.get())
            .filter(jsonObj -> jsonObj.containsKey("time"))//TODO del
            .mapToPair(jsonObj -> new Tuple2<Integer, JSONObject>(jsonObj.getInteger("time"), jsonObj))//JavaPairDStream<Integer, JSONObject> 
            .groupByKey()//JavaPairDStream<Integer, Iterable<JSONObject>> 
            .mapToPair(time_jsonIt_tuple -> new Tuple2<>(time_jsonIt_tuple._1, findMax3ByKey(time_jsonIt_tuple._2, "key1", "key2")))   //JavaPairDStream<Integer, JSONObject> 
            .print();
    }
    
    

    /*{"time":1000, "key1": 100}
    {"time":1000, "key1": 99}
    {"time":1000, "key1": 108}
    {"time":1001, "key1": 100}
    {"time":1001, "key1": 109}
    {"time":1001, "key1": 101}
    找到 {"time":1000, "key1": 108} 和 {"time":1001, "key1": 109}
    */
    static void findMax2(JavaInputDStream<ConsumerRecord<String, String>> lines)
    {
        lines.map(line -> parseObject(line.value()))
                .filter(Optional::isPresent)
                .map(opJsonObj -> opJsonObj.get())
                .filter(jsonObj -> jsonObj.containsKey("time"))//TODO del
                .mapToPair(jsonObj -> new Tuple2<Integer, JSONObject>(jsonObj.getInteger("time"), jsonObj))//JavaPairDStream<Integer, JSONObject> 
                .groupByKey()//JavaPairDStream<Integer, Iterable<JSONObject>> 
                .mapToPair(time_jsonIt_tuple -> new Tuple2<>(time_jsonIt_tuple._1, findMax2ByKey(time_jsonIt_tuple._2, "key1")))   //JavaPairDStream<Integer, JSONObject> 
                .print();
    }
    
    private static JSONObject findMax2ByKey(Iterable<JSONObject> jsonObjIt, String key)
    {
        return Stream.of(Iterables.toArray(jsonObjIt, JSONObject.class)).sorted((json1, json2) -> json2.getInteger(key) - json1.getInteger(key)).findFirst().get();
    }
    
    
    private static JSONObject findMax3ByKey(Iterable<JSONObject> jsonObjIt, String... keys)
    {
        return Stream.of(Iterables.toArray(jsonObjIt, JSONObject.class)).sorted(compare(keys)).findFirst().get();
    }
    
    private static JSONObject findMax4ByKey(Iterable<JSONObject> jsonObjIt, String jsonArrayName, String subKey)
    {
        return Stream.of(Iterables.toArray(jsonObjIt, JSONObject.class)).sorted(compareWithJson(jsonArrayName, subKey)).findFirst().get();
    }

    private static Comparator<? super JSONObject> compareWithJson(String jsonArrayName, String subKey)
    {
        return (json1, json2) -> ComparisonChain.start().compare(sumFrmJsonData(json2, "data", "key2"), sumFrmJsonData(json1, "data", "key2")).result();
    }

    private static Comparator<? super JSONObject> compare(String... keys)
    {
        return (json1, json2) -> {
            ComparisonChain chain = ComparisonChain.start();  
            for(String key : keys)
            {
                chain.compare(json2.getInteger(key), json1.getInteger(key));
            }
            return chain.result();
        };
    }

    //{"key": 100}
    //{"key": 200}
    //{"key": 300}
    //找到{"key": 300}
    static void findMax(JavaInputDStream<ConsumerRecord<String, String>> lines)
    {
        lines.map(line -> Optional.of(JSONObject.parseObject(line.value())))
              .filter(Optional::isPresent)
              .map(opJsonObj -> opJsonObj.get())
              
              .mapToPair(jsonObj -> new Tuple2<Integer, JSONObject>(jsonObj.getInteger("key"), jsonObj))
              .reduce((tuple1, tuple2) -> tuple1._1 > tuple2._1 ? tuple1 : tuple2)
              
              .print();
    }

    private static Optional<JSONObject> parseObject(String line)
    {
        try
        {
            return Optional.of(JSONObject.parseObject(line));
        }
        catch(Exception e)
        {
            return Optional.empty();
        }
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
        
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
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
    
    //{"time":1000, "key1": 100, "data": [{"key2":2, "other":"aaa"}, {"key2":2, "other":"aaa"}]}
    private static int sumFrmJsonData(JSONObject json, String jsonArrayName, String subKey)
    {
        JSONArray jsonArray = json.getJSONArray(jsonArrayName);
        
        int sum = 0;
        if(jsonArray != null)
        {
            for(int i = 0; i < jsonArray.size(); i++)
            {
                sum += jsonArray.getJSONObject(i).getInteger(subKey);
            }
        }
        
        return sum;
    }
    
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
