package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— reduceByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 3), ("a", 2), ("b", 4)
    ))
    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //Scala语言中一般的聚合操作都是两两聚合，spark是基于Scala开发的，所以它的聚合也是两两聚合
    //[1,2,3] => [3,3] => [6]

    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => {
      //reduceByKey中如果一个key的数据只有一个，是不会参与运算的
      println(s"x = ${x},y = ${y}")
      x + y
    })

    println(reduceRDD.collect().mkString(",")) //(a,6),(b,4)
    sc.stop()
  }
}
