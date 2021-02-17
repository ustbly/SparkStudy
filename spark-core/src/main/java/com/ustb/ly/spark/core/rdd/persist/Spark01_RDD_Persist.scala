package com.ustb.ly.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Spark", "Hello Scala")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(str => str.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    resRDD.collect().foreach(println)

    println("--------------------")
    val rdd1: RDD[String] = sc.makeRDD(list)

    val flatRDD1: RDD[String] = rdd1.flatMap(str => str.split(" "))

    val mapRDD1: RDD[(String, Int)] = flatRDD1.map((_, 1))

    val resRDD1: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()

    resRDD1.collect().foreach(println)

    sc.stop()
  }
}
