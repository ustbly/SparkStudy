package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(6, 2, 4, 5, 1, 3), 2)

    val sortByRDD: RDD[Int] = rdd.sortBy(num => num)

    sortByRDD.saveAsTextFile("output")

    sc.stop()
  }
}
