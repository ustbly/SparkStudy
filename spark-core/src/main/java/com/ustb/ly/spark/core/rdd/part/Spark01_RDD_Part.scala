package com.ustb.ly.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
 * 自定义分区器
 */
object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "kobe"),
      ("nba", "james"),
      ("cba", "sunyue"),
      ("wnba", "lucy")
    ))

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")

    sc.stop()
  }


  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }

}
