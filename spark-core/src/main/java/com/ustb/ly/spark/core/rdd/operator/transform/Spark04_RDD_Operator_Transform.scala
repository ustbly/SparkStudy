package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— flatMap

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

    val flatRDD: RDD[Int] = rdd.flatMap(
      list => { //此处的list是指集合中的元素，比如List(1,2)和List(3,4)
        list //此处的list是指将上面list拆开之后重新封装的List集合
      }
    )

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
