package com.bankcomm.pccc.onesight.demo.sparkstreaming;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
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
 * 5. KafkaDirectWordCount
 * 从kafka输入的实时统计数字
 * next：KafkaDirectAccWordCount
 * @author A175703
 * 
 * Direct方式从kafka读取数据，计算并打印
 * ★官方文档：http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 * 另外一种写法可以参考：http://blog.csdn.net/jacklin929/article/details/53888763
 */
public class KafkaDirectWordCount {
    private static void runSparkStreamingDemo() throws InterruptedException {
        /**      
         * 配置SparkConf：
           1，至少两条线程因为Spark Streaming应用程序在运行的时候至少有一条线程用于
                               不断地循环接受程序，并且至少有一条线程用于处理接受的数据（否则的话有线程用于处理数据，随着时间的推移内存和磁盘都会不堪重负）
           2，对于集群而言，每个Executor一般肯定不止一个线程，那对于处理SparkStreaming
                            应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的Core是最佳的
                            （一个段子分配为奇数个Core表现最佳，例如3个，5个，7个Core等）*/      
        SparkConf sparkConf = new SparkConf()
                            .setMaster("local[2]")
                            .setAppName("KafkaDirectWordCount");
        
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> logtopicsSet = Sets.newHashSet("sparktest");
         
        Map<String, Object> kafkaParams = Maps.newHashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "group1");//kafka consumer group相关
        kafkaParams.put("auto.offset.reset", "latest");//初始偏移或者偏移不存在时（如被删除）的处理方式
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("enable.auto.commit", false);//If true the consumer's offset will be periodically committed in the background.

        JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(
                sc,
                LocationStrategies.PreferConsistent(),//spark与kafka consumer的缓存策略，与获得更高的性能相关。大多数情况下使用PreferConsistent
                ConsumerStrategies.<String, String>Subscribe(logtopicsSet, kafkaParams)
        );
      
        /**
         * kafka的消息可靠性是依赖于offset是如何定义的。默认情况下，使用spark output operation，是at-least-once。
         * 如果想要获得exactly-once的可靠性 ，需要自定义存储offset规则。
         * 官方文档介绍了三种方式：
         * 1. checkpoint：会得到重复的输出，因此要求操作必须是幂等的（所以“交易”这种操作就不行）。
         *        而且在application代码修改的后会致失败，比如a) 启动报错，反序列异常  b) 启动正常，但运行的仍然是上一次程序的代码。
         * 2. kafka自身：当知道输出已经存储后，使用commitAsync提交offset给kafka。好处是kakfa是持久化的，也不受程序代码变化的影响。缺点是kafka不是事务的，所以仍然需要保证输出是幂等的。
         * 3. 自己的datastore：可以将offset和结果保存在同一个事务中来得到事务级别的可靠性。也可以自己控制失败时回滚或者跳过offset等，灵活性很高。
         *        使用这一条时，在createDirectStream时需要在ConsumerStrategies中Assign自己保存的offset和kafka参数。
         * 本示例代码使用了第二种方式。
         */
        lines.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) lines.inputDStream()).commitAsync(offsetRanges);
        });
        
        JavaPairDStream<String, Integer> counts = 
                lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y);  
        
        counts.print();
        sc.start();
        sc.awaitTermination();
    }
    
    public static void main(String[] args) throws InterruptedException {
        runSparkStreamingDemo();
    }
}
