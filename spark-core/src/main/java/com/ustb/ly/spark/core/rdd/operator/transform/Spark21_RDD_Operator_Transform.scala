package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— join
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("c", 4), ("b", 5), ("a", 6)))

    //join:两个不同的数据源，相同的key的value会连接在一起，形成元组
    //     如果这两个数据源中有的key匹配不上，那么数据将不会出现在结果中
    //     如果两个数据源中key有多个相同的，那么会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)
    sc.stop()
  }
}
