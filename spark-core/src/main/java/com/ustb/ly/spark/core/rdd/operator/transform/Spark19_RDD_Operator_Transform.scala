package com.ustb.ly.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 —— Key-Value 类型 —— combineByKey
    // TODO 获取相同key的数据的value的平均值
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    //combineByKey:方法需要三个参数
    //第一个参数表示：将相同的key的第一个数据进行结构的转换，实现操作
    //第二个参数表示：分区内的计算规则
    //第三个参数表示：分区间的计算规则
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => { //这里的t就是上面的(v,1)代表的元组（相同的key的第一个数据转换结构之后的），v就是相同key但除了第一个元组的v
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD: RDD[(String, Int)] = combineRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
