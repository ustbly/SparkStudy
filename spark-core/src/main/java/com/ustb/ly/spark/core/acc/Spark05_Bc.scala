package com.ustb.ly.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Bc")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    /*   val rdd2 = sc.makeRDD(List(
         ("a",4),
         ("b",5),
         ("c",6)
       ))*/

    //join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //joinRDD.collect().foreach(println)

    //(a,1),    (b,2),    (c,3)
    //(a,(1,4)),(b,(2,5)),(c,(3,6))

    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    rdd1.map {
      case (w, c) => {
        val i: Int = map.getOrElse(w, 0)
        (w, (c, i))
      }
    }.collect().foreach(println)
    sc.stop()
  }

}
