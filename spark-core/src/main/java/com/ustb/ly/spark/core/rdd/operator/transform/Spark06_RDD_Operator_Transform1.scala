package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— groupBy
    val rdd = sc.makeRDD(List("Hello", "Scala", "Hadoop", "Spark"))
    /*
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(
      str => {
        str.charAt(0)
      }
    )
    */

    //分组和分区没有必然的关系
    //会出现数据被打散的情况，也就是会有shuffle的过程。（与Spark01_RDD_Operator_Transform_Part.scala对比）
    val groupRDD = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)


    sc.stop()
  }
}
