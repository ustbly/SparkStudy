package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— flatMap

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val flatMapRDD: RDD[String] = rdd.flatMap(
      str => {
        str.split(" ")
      }
    )
    flatMapRDD.collect().foreach(println)
    sc.stop()
  }
}
