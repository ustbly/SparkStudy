package com.ustb.ly.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    //[*]代表当前系统的最大可用核数，不写默认使用单线程模拟单核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //RDD的并行度 & 分区
    //makeRDD方法可以传递第二个参数，这个参数代表分区的数量
    //第二个参数也可以不传递，那么makeRDD方法会使用默认值：defaultParallelism（默认并行度）
    //  scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //  spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    //  如果获取不到，那么使用totalCores属性，这个参数取值为当前运行环境的最大可用核数

    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    4
    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    //TODO 关闭环境
    sc.stop()
  }
}
