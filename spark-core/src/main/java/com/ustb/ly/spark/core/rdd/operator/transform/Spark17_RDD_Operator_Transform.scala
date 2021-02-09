package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— aggregateByKey
    //  取出每个分区内相同 key 的最大值然后分区间相加
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ), 2)

    //(a,[1,2]),(a,[3,4]) => (a,2),(a,4) => (a,6)
    //aggregateByKey存在函数柯里化，有两个参数列表
    //第一个参数列表，需要传递一个参数，表示为初始值
    //    主要用于碰见第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传递两个参数
    //    第一个参数表示分区内计算规则
    //    第二个参数表示分区间计算规则

    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => (x + y)
    )

    println(aggRDD.collect().mkString(","))

    sc.stop()
  }
}
