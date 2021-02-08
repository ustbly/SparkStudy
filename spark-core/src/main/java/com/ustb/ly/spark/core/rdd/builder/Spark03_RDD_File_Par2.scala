package com.ustb.ly.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    //[*]代表当前系统的最大可用核数，不写默认使用单线程模拟单核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD

    /*
      1234567@@ => 012345678
      89@@      => 9101112
      0         => 13

      14 / 2 = 7 (byte)
      14 / 7 = 2 (分区)

      0分区 => [0,7] => 1234567
      1分区 => [7,14] => 89回车换行0

     */


    //如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
