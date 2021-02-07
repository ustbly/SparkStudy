package com.ustb.ly.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")


    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

    val res: RDD[(String, Int)] = wordGroup.map {
      case (str, list) => {
        list.reduce {
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        }
      }
    }

    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }
}
