package com.ustb.ly.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //从服务器日志数据 apache.log 中获取每个时间段访问量。
    // TODO 算子 —— groupBy
    val rdd = sc.textFile("datas/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val fields: Array[String] = line.split(" ")
        val date: String = fields(3)

        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date1: Date = sdf.parse(date)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date1)
        (hour, 1)
      }
    ).groupBy(_._1)

    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
