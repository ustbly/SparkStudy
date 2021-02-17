package com.ustb.ly.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("*****************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("*****************************")
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.dependencies)
    println("*****************************")
    val res: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(res.dependencies)
    println("*****************************")
    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }

}
