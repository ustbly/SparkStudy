package com.ustb.ly.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))
    //此处的foreach其实是各个Executor端的数据收集到Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)

    println("-------------------")
    //此处的foreach其实是Executor端内存数据的打印
    rdd.foreach(println)

    //算子：Operator(操作)
    //      RDD的方法和Scala集合对象的方法不一样
    //      集合对象的方法都是在同一个节点的内存中完成的
    //      RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    //      为了区分不同的处理效果，所以将RDD的方法称为算子
    //      RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行的
    sc.stop()
  }
}
