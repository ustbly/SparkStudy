package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— map

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //记录每个数据存放在哪个分区
    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        //1，2，3，4
        //(0,1)(1,2)(2,3)(3,4)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )
    mpiRDD.collect().foreach(println)

    sc.stop()
  }
}
