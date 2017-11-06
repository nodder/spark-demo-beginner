package com.bankcomm.pccc.onesight.demo.sparkstreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 3. SocketWordCount
 * 从socket读取文本，并实时计算。与nc工具配合使用。
 * next：SocketWordCountJava8
 * @author A175703
 */
public class SocketWordCount {
    @SuppressWarnings("serial")
    private static void runSparkStreamingDemo() throws InterruptedException {
        /**
         * 第一步：配置SparkConf：
           1，至少两条线程(local后面的数字)因为Spark Streaming应用程序在运行的时候至少有一条线程用于
                               不断地循环接受程序，并且至少有一条线程用于处理接受的数据（否则的话有线程用于处理数据，随着时间的推移内存和磁盘都会不堪重负）
           2，对于集群而言，每个Executor一般肯定不止一个线程，那对于处理SparkStreaming
                            应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的Core是最佳的
                            （一个段子分配为奇数个Core表现最佳，例如3个，5个，7个Core等）*/
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        
        /**
         * 
         * 创建上下文对象，第二个参数表示batch interval，即每收集多长时间的数据来划分一个batch。
         */
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        
        /**
         * 创建Spark Streaming 输入数据来源：input Stream
         * 1.数据输入来源可以基于File、HDFS、Flume、Kafka、Socket
         * 
         * 2.在这里我们指定数据来源于网络Socket端口,
         * Spark Streaming连接上该端口并在运行的时候一直监听该端口的数据（当然该端口服务首先必须存在，并且在后续会根据业务需要不断的有数据产生）。
         * 
         * 有数据和没有数据 在处理逻辑上没有影响（在程序看来都是数据）
         * 
         * 3.如果经常在每间隔5秒钟 没有数据的话,不断的启动空的job其实是会造成调度资源的浪费。因为 并没有数据需要发生计算。
         * 所以企业级生产环境的代码在具体提交Job前会判断是否有数据，如果没有的话，就不再提交Job；
         */
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        
        /**
         * 每隔一秒就有一个RDD发送过来，包含了这一秒中的数据。RDD的元素类型为String，在这里是一行一行的文本
         */
        JavaDStream<String> words =  lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });
        
        JavaPairDStream<String, Integer>  wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        
        /**
         * 此处的print并不会直接触发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的。
         * 具体是否真正触发Job的运行是基于设置的Duration时间间隔的触发的。
         * 
         * 
         * Spark应用程序要想执行具体的Job对DStream就必须有output Stream操作。
         * ouput Stream 有很多类型的函数触发,类print、savaAsTextFile、saveAsHadoopFiles等，最为重要的是foreachRDD，因为Spark Streamimg处理的结果一般都会
         * 放在Redis、DB、DashBoard等上面，foreachRDD主要
         * 就是用来 完成这些功能的，而且可以随意的自定义具体数据放在哪里。
         * 
         */
        wordsCount.print();

        /**
         * start方法表示启动执行整个Spark Streaming Application
         */
        ssc.start();

        ssc.awaitTermination();
        ssc.close();
    }
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
