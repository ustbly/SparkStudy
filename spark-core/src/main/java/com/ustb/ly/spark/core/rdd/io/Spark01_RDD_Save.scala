package com.ustb.ly.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
 * 保存文件
 */
object Spark01_RDD_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
