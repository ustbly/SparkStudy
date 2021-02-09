package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— repartition
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //coalesce算子可以扩大分区的，如果不进行shuffle操作，是没有意义的，不起作用
    //如果想要实现扩大分区的效果，需要使用shuffle操作
    //val newRDD: RDD[Int] = rdd.coalesce(3,true)

    //spark提供了一个简化的操作
    //缩减分区：coalesce，如果想要数据均衡，可以使用shuffle
    //扩大分区：repartition，底层代码调用的就是coalesce，而且肯定采用shuffle
    val newRDD: RDD[Int] = rdd.repartition(3)
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
