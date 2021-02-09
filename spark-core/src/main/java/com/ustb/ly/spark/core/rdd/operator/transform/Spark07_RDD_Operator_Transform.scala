package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— filter
    //过滤得到偶数
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val filterRDD: RDD[Int] = rdd.filter(
      num => {
        num % 2 == 0
      }
    )

    filterRDD.collect().foreach(println)

    sc.stop()
  }
}
