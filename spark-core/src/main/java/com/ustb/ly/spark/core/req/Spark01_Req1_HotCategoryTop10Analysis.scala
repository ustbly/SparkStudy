package com.ustb.ly.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

//TODO TOP10热门品类分析
object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)
    //1.读取原始日志数据


    //2.统计品类的点击数量：（品类ID，点击数量）


    //3.统计品类的下单数量：（品类ID，下单数量）


    //4.统计品类的支付数量：（品类ID，支付数量）


    //5.将品类排序，并且取前十名
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
    //（品类ID，（点击数量，下单数量，支付数量））

    //6.将结果采集到控制台打印出来

    sc.stop()

  }
}
