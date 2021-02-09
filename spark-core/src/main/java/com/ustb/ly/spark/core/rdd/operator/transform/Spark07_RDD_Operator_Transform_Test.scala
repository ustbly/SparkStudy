package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— filter
    //从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
    val rdd = sc.textFile("datas/apache.log")

    val filterRDD: RDD[String] = rdd.filter(
      line => {
        val fields: Array[String] = line.split(" ")
        fields(3).substring(0, 10).equals("17/05/2015")
        //fields(3).startsWith("17/05/2015")
      }
    )

    filterRDD.collect().foreach(println)

    sc.stop()
  }
}
