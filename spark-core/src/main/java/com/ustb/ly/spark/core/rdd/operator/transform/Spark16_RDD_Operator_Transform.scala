package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— groupByKey和groupBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 3), ("a", 2), ("b", 4)
    ))

    //groupByKey:将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //           元组第一个元素就是key
    //           元组第二个元素就是相同key的value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //(a,CompactBuffer(1, 3, 2)),(b,CompactBuffer(4))
    println(groupRDD.collect().mkString(","))

    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(t => t._1)
    //(a,CompactBuffer((a,1), (a,3), (a,2))),(b,CompactBuffer((b,4)))
    println(groupRDD1.collect().mkString(","))
    sc.stop()
  }
}
