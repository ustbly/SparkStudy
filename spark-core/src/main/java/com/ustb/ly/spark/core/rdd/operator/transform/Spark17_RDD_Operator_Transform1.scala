package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— aggregateByKey
    //  取出每个分区内相同 key 的最大值然后分区间相加
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => (x + y)
    )

    //分区内和分区间也可以进行相同的逻辑操作，且可以使用Scala的至简原则进行简化
    /*
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => (x + y),
      (x, y) => (x + y)
    )

    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
    */
    println(aggRDD.collect().mkString(",")) //(b,8),(a,8)

    sc.stop()
  }
}
