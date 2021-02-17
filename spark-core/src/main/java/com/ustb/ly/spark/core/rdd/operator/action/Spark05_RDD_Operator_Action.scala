package com.ustb.ly.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")

    //saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
