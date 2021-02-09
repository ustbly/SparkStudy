package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— 双Value类型

    //java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    //要求两个数据源的分区数量保持一致
    //Can only zip RDDs with same number of elements in each partition
    //要求两个数据源分区的数据量保持一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    //拉链:(1,3),(2,4),(3,5),(4,6)
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()
  }
}
