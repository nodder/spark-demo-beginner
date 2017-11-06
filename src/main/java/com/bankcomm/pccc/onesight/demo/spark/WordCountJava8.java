package com.bankcomm.pccc.onesight.demo.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 2. WordCountJava8
 * WordCount的java8版本
 * next：SocketWordCount
 * @author A175703
 */
public final class WordCountJava8 {
    private static final Pattern SPACE = Pattern.compile(" ");

    private static void runSparkDemo() {
        SparkConf sparkConf = new SparkConf()
                                    .setAppName("JavaWordCount")
                                    .setMaster("local");
      
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)){
            sc.textFile("demo.txt")
              .flatMap(line -> Arrays.asList(SPACE.split(line)).iterator())
              .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
              .reduceByKey((v1, v2) -> v1 + v2)
              .foreach(wordAndCount -> System.out.println(wordAndCount._1 + " appeared " + wordAndCount._2 + " times."));
            
            sc.stop();
        } 
    }
    
    public static void main(String[] args) throws Exception {
        runSparkDemo();
    }
}