package com.ustb.ly.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Bc {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Bc")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))


    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (w, c) => {
        //访问广播变量
        val i: Int = bc.value.getOrElse(w, 0)
        (w, (c, i))
      }
    }.collect().foreach(println)
    sc.stop()
  }

}
