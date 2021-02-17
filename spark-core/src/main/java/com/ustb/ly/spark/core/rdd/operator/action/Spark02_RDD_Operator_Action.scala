package com.ustb.ly.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO 行动算子

    //reduce
    val res: Int = rdd.reduce(_ + _)
    println(res)

    //collect
    //方法会将不同分区的数据按照分区顺序采集到Driver端的内存中，形成数组
    val array: Array[Int] = rdd.collect()
    println(array.mkString(","))

    //count:数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    //first:获取数据源中数据的第一个
    val first: Int = rdd.first()
    println(first)

    //take:获取前N个数据
    val take: Array[Int] = rdd.take(3)
    println(take.mkString(","))

    //takeOrdered:数据排序后，取前N个数据
    val rdd1: RDD[Int] = sc.makeRDD(List(4, 2, 3, 1))
    val takeOrdered: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse) //降序
    println(takeOrdered.mkString(","))

    sc.stop()
  }
}
