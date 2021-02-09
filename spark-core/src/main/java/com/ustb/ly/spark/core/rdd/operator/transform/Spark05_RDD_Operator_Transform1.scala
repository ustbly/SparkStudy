package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— glom
    //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()
    //[1,2],[3,4]
    //[2],[4]
    //[6]
    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )

    println(maxRDD.collect().sum)
    //println(maxRDD.sum().toInt)

    sc.stop()
  }
}
