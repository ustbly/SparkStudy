package com.ustb.ly.spark.core.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD = rdd.map(
      num => {
        //使用累加器
        sumAcc.add(num)
      }
    )

    //获取累加器的值
    //少加：转换算子中调用累加器，如果没有调用行动算子的话，那么会出现少加的情况
    //多加：转换算子中调用累加器，如果多次调用行动算子，那么会出现多加的情况
    //一般情况下，累加器会放在行动算子中进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)

    sc.stop()
  }
}
