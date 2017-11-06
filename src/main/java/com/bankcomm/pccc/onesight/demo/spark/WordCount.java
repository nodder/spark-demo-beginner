package com.bankcomm.pccc.onesight.demo.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 1. WordCount
 * 读取文本并统计单词数
 * next：WordCountJava8
 * @author A175703
 */
public final class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    @SuppressWarnings("serial")
    private static void runSparkDemo() {
        /**
         * 创建sparkconf 对象，设置spark 应用的配置信息
                             使用setMaster()可以设置spark 应用程序要连接的spark 集群的master节点的url，如果设置为local 则代表，在本地运行*/
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        
       /**  
        * 创建 Javasparkcontext对象。在 Spark中， Sparkcontext是 spark所有功能的入口，你无论是用 java、scala或是python编写，都必须要有一个 sparkContext。
                          它的主要作用，包括初始化 spark应用程序所需的一些核心组件，包括调度器( DAGschedule 、 Taskscheduler ) ，还会去到 sparkMaster节点上进行注册，等等
                          一句话， SparkContext是 spark 应用中，可以说是最最重要的一个对象
                          但是呢，在 spark中，编写不同类型的 spark 应用程序，使用的 sparkcontext是不同的，如果使用 scala，就是原生的 sparkcontext对象
                          但是如果使用 Java，那么就是 JavaSparkContext对象
                          如果是开发 spark SQL程序，那么就是 SQLcontext, Hivecontext
                          如果是开发 sparks Streaming程序，那么就是它独有的 sparkcontext
                          以此类推*/
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

      /**
       * 第三步：要针对输入源（如hdfs文件、本地文件等等），创建一个初始的 RDD。
                        输入源中的数据会打散，分配到RDD的每个partition 中，从而形成一个初始的分布式的数据集
                       我们这里因为是本地测试，所以是针对本地文件(sparkcontext.textFile方法)
                      在 Java中，创建的普通RDD，都叫做 JavaRDD
                      对于hdfs 或者本地文件，创建的 RDD的每一个元素就相当于是文件里的一行*/
        JavaRDD<String> lines = sc.textFile("demo.txt");

        /**
         * 对初始 RDD 进行transformation操作，也就是一些计算操作。
                             通常操作会通过创建function，并配合RDD的map、flatMap 等算子来执行。
                             先将每一行拆分成单个的单词。
           FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型。在这里输入是每一行文本
           flatMap算子的作用，是将RDD的一个元素，拆分成一个或多个元素*/
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        /**
         * 接着，需要将每一个单词映射为(单词， 1)的这种格式。
           mapToPair，其实就是将每个元素，映射为一个( v1 , v2)这样的 Tuple2类型的元素
           mapToPair这个算子，要求的是与PairFunction 配合使用，第一个泛型参数表示输入类型
                            第二个和第三个泛型参数，表示输出的 Tuple2中两个值的参数类型
           JavaPairRDD的两个泛型参数，分别代表了 tuple元素中两个值的参数类型*/
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        /**
         * 接着，需要以单词作为key，统计每个单词出现的次数。
           reduceByKey算子，对每个key对应的value，都进行reduce操作。
                             比如JavaPairRDD 中有几个元素，分别为(hello , 1) (hello , 1) (world , 1)，
           reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
                            最后返回的JavaPairRDD中的元素，也是tuple。其中第一个值是key ,第二个值是reduce之后的结果，即每个单词出现的次数。*/
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        /**
         * 到这里为止，我们通过几个 spark 算子操作，己经统计出了单词的次数。
           flatMap、mapToPair、 reducesyKey这些操作，都是transformation操作，都是中间操作，并不能使程序执行。
                             在transformation操作之后，必须有action操作，例如foreach，才能触发程序执行。
         */
        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordAndCount) throws Exception {
                System.out.println(wordAndCount._1 + " appeared " + wordAndCount._2 + " times.");
            }
        });

//        anotherAction(counts);
        sc.stop();
        sc.close();
    }

    static void anotherAction(JavaPairRDD<String, Integer> counts) {
//        counts.saveAsTextFile("G:\\ceshi\\bigdata\\spark\\wordcount\\output\\out1");
         List<Tuple2<String, Integer>> output = counts.collect();
         for (Tuple2<?,?> tuple : output) {
         System.out.println(tuple._1() + ": " + tuple._2());
         }
    }
    
    public static void main(String[] args) throws Exception {
        runSparkDemo();
    }
}