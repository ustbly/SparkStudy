package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— coalesce
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //coalesce方法默认情况下不会将分区的数据打乱重新组合（shuffle）
    //这种情况下的缩减分区可能会造成数据不均衡，出现数据的倾斜
    //val newRDD: RDD[Int] = rdd.coalesce(2)

    //如果想让数据均衡，可以使用shuffle进行处理
    val newRDD: RDD[Int] = rdd.coalesce(2, true)
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
