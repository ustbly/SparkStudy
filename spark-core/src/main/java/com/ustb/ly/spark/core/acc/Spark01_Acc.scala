package com.ustb.ly.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //reduce:分区内计算，分区间计算
    //val res: Int = rdd.reduce(_ + _)
    //println(res)


    var sum = 0

    rdd.foreach(
      num => {
        sum += num
      })

    println("sum = " + sum)

    sc.stop()
  }
}
