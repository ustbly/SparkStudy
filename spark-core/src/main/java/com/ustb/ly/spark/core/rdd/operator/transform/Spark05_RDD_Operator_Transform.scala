package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— glom
    //List => Int
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.foreach(arr => {
      println(arr.mkString(","))
    })


    sc.stop()
  }
}
