package com.ustb.ly.spark.core.rdd.part

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
 * 加载（读取）文件
 */
object Spark02_RDD_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[String] = sc.textFile("output1")
    println(rdd1.collect().mkString(","))

    val rdd2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
    println(rdd2.collect().mkString(","))

    val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output3")
    println(rdd3.collect().mkString(","))

    sc.stop()
  }

}
