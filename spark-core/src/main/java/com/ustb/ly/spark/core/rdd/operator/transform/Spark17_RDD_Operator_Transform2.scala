package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— aggregateByKey
    //  取出每个分区内相同 key 的最大值然后分区间相加
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    //rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    //如果聚合计算时，分区内和分区间的计算规则相同，spark提供了简化的方法

    //(b,12)
    //(a,9)
    rdd.foldByKey(0)(_ + _).collect().foreach(println)


    sc.stop()
  }
}
