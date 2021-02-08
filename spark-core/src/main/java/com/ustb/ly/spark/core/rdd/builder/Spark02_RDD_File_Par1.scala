package com.ustb.ly.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    //[*]代表当前系统的最大可用核数，不写默认使用单线程模拟单核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //TODO 数据分区的分配
    //1.数据以行为单位读取
    //  spark读取文件，采用的是Hadoop的方式读取，所以一行一行读取，和字节数没有关系

    //2.数据读取时以偏移量为单位，偏移量不会被重新读取
    /*
        1@@   =>  012
        2@@   =>  345
        3     =>  6
     */

    //3.数据分区的偏移量范围的计算
    /*
        0 => [0,3] => 12
        1 => [3,6] => 3
        2 => [6,7] =>
     */
    //4.最后结果：[1,2],[3],[]
    //分区数量的计算方式：
    //  totalSize = 7
    //  goalSize = 7 / 2 = 3（byte）（每个分区放三个字节）

    //  7 / 3 = 2 .. 1 (根据1.1原则) + 1 = 3 分区
    val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
