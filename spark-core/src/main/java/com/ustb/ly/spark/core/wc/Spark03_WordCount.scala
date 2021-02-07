package com.ustb.ly.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")


    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    //Spark框架提供了更多的功能，可以将分组和聚合使用一个方法来实现
    //reduceByKey：相同的key的数据，可以对value进行reduce聚合
    //wordToOne.reduceByKey((x,y)=>{x + y})
    //wordToOne.reduceByKey((x,y)=>x + y)
    val res: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }
}
