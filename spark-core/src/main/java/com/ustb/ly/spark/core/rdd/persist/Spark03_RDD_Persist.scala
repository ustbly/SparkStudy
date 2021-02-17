package com.ustb.ly.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Spark", "Hello Scala")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(str => str.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("$$$$$$$$$$$$$$$$$$$$$$$$$")
      (word, 1)
    })

    //cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件中，需要更改存储级别
    //mapRDD.cache()    //将map阶段产生的RDD的数据放到内存中保存，实现数据的重用
    mapRDD.persist(StorageLevel.DISK_ONLY) //不需要指定路径，保存成临时文件，作业执行完毕之后会立即删除文件

    //持久化操作必须在行动算子执行时完成的

    //RDD对象的持久化操作不一定是为了重用
    //在数据执行较长，或数据比较重要的场合也可以采用持久化操作

    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    resRDD.collect().foreach(println)

    println("--------------------")

    val resRDD1: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    resRDD1.collect().foreach(println)

    sc.stop()
  }
}
