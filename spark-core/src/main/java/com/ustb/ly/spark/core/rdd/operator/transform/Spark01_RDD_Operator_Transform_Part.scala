package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— map
    //将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //[1,2],[3,4]
    rdd.saveAsTextFile("output")
    val mapRDD: RDD[Int] = rdd.map(_ * 2)
    //[2,4],[6,8]
    mapRDD.saveAsTextFile("output1")
    sc.stop()
  }
}
