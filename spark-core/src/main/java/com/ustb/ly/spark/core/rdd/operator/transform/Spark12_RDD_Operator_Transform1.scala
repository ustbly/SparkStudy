package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数为false表示降序
    //sortBy默认情况下，不会改变分区（分区数量不变）。但是中间存在shuffle操作
    val sortByRDD: RDD[(String, Int)] = rdd.sortBy(
      t => {
        t._1.toInt
      }, false //降序
    )

    sortByRDD.collect().foreach(println)

    sc.stop()
  }
}
