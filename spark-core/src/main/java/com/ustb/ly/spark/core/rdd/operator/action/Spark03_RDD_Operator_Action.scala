package com.ustb.ly.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //TODO 行动算子

    //val res: Int = rdd.aggregate(0)(_ + _, _ + _)

    //aggregateByKey：初始值只会参与分区内的计算  13 + 17 = 30
    //aggregate：初始值不仅会参与分区内的计算，还参与分区间的计算  13 + 17 + 10 = 40
    val res: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(res)

    //fold:当aggregate分区间和分区内规则一致时，可以采用fold进行简化
    val i: Int = rdd.fold(10)(_ + _)
    println(i)

    sc.stop()
  }
}
