package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— leftOuterJoin & rightOuterJoin
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("b", 5), ("a", 6)))

    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)))
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("b", 5), ("a", 6), ("c", 4)))
    /*
      (a,(1,Some(6)))
      (b,(2,Some(5)))
      (c,(3,None))
     */
    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    leftJoinRDD.collect().foreach(println)
    /*
      (a,(Some(1),6))
      (b,(Some(2),5))
      (c,(None,4))
    */
    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd3.rightOuterJoin(rdd4)
    rightJoinRDD.collect().foreach(println)
    sc.stop()
  }
}
