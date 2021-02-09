package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //sample算子需要传递三个参数：
    //1.第一个参数表示：抽取数据后是否将数据返回
    //               true（返回：（泊松算法）） false（不返回：（伯努利算法））
    //2.第二个参数表示：
    //               如果抽取不放回的场合：数据源中每条数据被抽取的概率,基准值的概念
    //               如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
    //3.第三个参数表示：抽取数据时随机算法的种子
    //               如果不传递第三个参数，那么使用的是当前系统的时间
    /*val sampleRDD: RDD[Int] = rdd.sample(
      false,
      0.4
      //1
    )*/

    val sampleRDD: RDD[Int] = rdd.sample(
      true,
      2
      //1
    )

    println(sampleRDD.collect().mkString(","))
    sc.stop()
  }
}
