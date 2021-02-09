package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— distinct
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    //map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    //(1,null),(2,null),(3,null),(4,null),(1,null),(2,null),(3,null),(4,null)
    //(1,null),(1,null)...
    //(null,null) => null...
    //(1,null) => 1...
    val distinctRDD: RDD[Int] = rdd.distinct()

    println(distinctRDD.collect().mkString(","))
    sc.stop()
  }
}
