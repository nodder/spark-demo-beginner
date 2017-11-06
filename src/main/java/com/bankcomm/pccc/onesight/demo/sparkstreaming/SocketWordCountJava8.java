package com.bankcomm.pccc.onesight.demo.sparkstreaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 4. SocketWordCountJava8
 * SocketWordCount的java8版本
 * next：KafkaDirectWordCount
 * @author A175703
 */
public class SocketWordCountJava8 {
    private static void runSparkStreamingDemo() throws InterruptedException {
        
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        
        try(JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1))){
            ssc.socketTextStream("localhost", 9999)
               .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
               .mapToPair(word -> new Tuple2<String,Integer>(word,1))
               .reduceByKey((v1, v2) -> v1 + v2)
               .print();

            ssc.start();
            ssc.awaitTermination();
        }
    }
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
