package com.ustb.ly.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    //[*]代表当前系统的最大可用核数，不写默认使用单线程模拟单核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源

    //path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
    //sc.textFile("D:\\IdeaProjects\\SparkStudy\\datas\\1.txt")
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")

    //path可以是文件的具体路径，也可以是目录名称，这时候读取该目录下所有文件
    //val rdd: RDD[String] = sc.textFile("datas")

    //path路径还可以使用通配符*
    //val rdd: RDD[String] = sc.textFile("datas/1*.txt")

    //path路径还可以是分布式文件存储系统路径，比如HDFS
    val rdd: RDD[String] = sc.textFile("hdfs://master:9000/test.txt")
    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
