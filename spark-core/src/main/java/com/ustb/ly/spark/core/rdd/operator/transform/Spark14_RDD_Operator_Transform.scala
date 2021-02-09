package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— partitionBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd1: RDD[(Int, Int)] = rdd.map(num => {
      (num, 1)
    })
    //RDD => PairRDDFunctions
    //partitionBy是PairRDDFunctions中的方法
    //是通过隐式转换（二次编译）实现的

    //partitionBy根据指定的分区规则对数据进行重新分区
    rdd1.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    sc.stop()
  }
}
