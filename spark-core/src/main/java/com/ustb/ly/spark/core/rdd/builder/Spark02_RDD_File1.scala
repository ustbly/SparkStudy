package com.ustb.ly.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    //[*]代表当前系统的最大可用核数，不写默认使用单线程模拟单核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //textFile：以行为单位来读取数据，读取的数据都是字符串
    //wholeTextFiles：以文件为单位读取数据，读取的结果为元组，第一个元素是文件路径，第二个是文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
