package com.ustb.ly.spark.core.acc

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello", "world", "hello"))
    //累加器：WordCount
    //创建累加器对象
    val wcAcc = new MyAccumulator()
    //向spark进行注册
    sc.register(wcAcc, "WordCountAcc")

    rdd.foreach(
      word => {
        //数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )

    //获取累加器的结果
    println(wcAcc.value)
    sc.stop()
  }

  /*
    自定义数据累加器：WordCount
      1.继承自AccumulatorV2，定义泛型
        IN：累加器输入的数据类型：String
        OUT：累加器输出的数据类型：mutable.Map[String, Long]

      2.重写方法(6个)
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var wcMap = mutable.Map[String, Long]()

    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    //重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
