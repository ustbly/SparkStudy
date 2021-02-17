package com.ustb.ly.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4),2)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))
    //TODO 行动算子 - countByValue & countByKey

    //val intToLong: collection.Map[Int, Long] = rdd.countByValue() //Map(4 -> 1, 1 -> 3)
    //println(intToLong)

    val stringToLong: collection.Map[String, Long] = rdd.countByKey() //Map(a -> 3)
    println(stringToLong)
    sc.stop()
  }
}
