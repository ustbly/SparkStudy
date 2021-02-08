package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— map

    //mapPartitions:可以以分区为单位进行数据转换操作
    //              但是会将整个分区的数据加载到内存中进行引用
    //              如果处理完的数据是不会被释放掉的，存在对象的引用
    //              在内存较小，数据量较大的场合下，容易出现内存溢出
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>")
        iter.map(_ * 2)
      }
    )

    mpRDD.collect().foreach(println)

    sc.stop()
  }
}
