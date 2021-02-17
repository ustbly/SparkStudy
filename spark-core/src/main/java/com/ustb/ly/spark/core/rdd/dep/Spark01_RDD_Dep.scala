package com.ustb.ly.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("*****************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*****************************")
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("*****************************")
    val res: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(res.toDebugString)
    println("*****************************")
    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }

}
