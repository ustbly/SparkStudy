package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— 案例实操
    //1.获取原始数据：时间戳，省份，城市，用户，广告
    val inputRDD: RDD[String] = sc.textFile("datas/agent.log")
    //2.将原始的数据进行结构的转换，方便统计。
    //时间戳，省份，城市，用户，广告 => ((省份，广告)，1)
    val proAndAdRDD: RDD[((String, String), Int)] = inputRDD.map(
      line => {
        val fields: Array[String] = line.split(" ")
        val pro: String = fields(1)
        val ad: String = fields(4)
        ((pro, ad), 1)
      }
    )

    //3.将转换结构后的数据进行分组聚合
    //((省份，广告)，1) => ((省份，广告)，sum)
    val sumRDD: RDD[((String, String), Int)] = proAndAdRDD.reduceByKey((i1, i2) => {
      i1 + i2
    })
    //4.将聚合的结果进行结构的转换
    //((省份，广告)，sum) => (省份，(广告，sum))
    val transformRDD: RDD[(String, (String, Int))] = sumRDD.map(
      t => {
        (t._1._1, (t._1._2, t._2))
      }
    )

    //5.将转换结构后的数据根据省份进行分组
    //(省份，[(广告A，sumA),(广告B，sumB),(广告C，sumC),])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = transformRDD.groupByKey()
    //6.将分组后的数据降序排列，取前三名
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(t => t._2)(Ordering.Int.reverse).take(3)
      }
    )
    //7.采集数据打印在控制台
    resRDD.collect().foreach(println)
    //8.关闭环境
    sc.stop()
  }
}
