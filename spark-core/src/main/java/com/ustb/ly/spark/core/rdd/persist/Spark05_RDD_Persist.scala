package com.ustb.ly.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * cache:将数据临时存储在内存中进行数据重用
 * persist：将数据临时存储在磁盘文件中进行数据重用
 * 设计到磁盘IO，性能较低，但是数据安全
 * 如果作业执行完毕，临时保存的数据文件就会丢失
 * checkpoint：将数据长久地保存在磁盘文件中进行数据重用
 * 涉及到磁盘IO，性能较低，但是数据安全
 * 为了保证数据安全，所以一般情况下，会独立执行作业
 * 为了提高效率，一般会和cache方法联合使用
 */
object Spark05_RDD_Persist {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("checkpointDir")

    val list = List("Hello Spark", "Hello Scala")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(str => str.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("$$$$$$$$$$$$$$$$$$$$$$$$$")
      (word, 1)
    })
    mapRDD.cache()
    mapRDD.checkpoint()


    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    resRDD.collect().foreach(println)

    println("--------------------")

    val resRDD1: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    resRDD1.collect().foreach(println)

    sc.stop()
  }
}
