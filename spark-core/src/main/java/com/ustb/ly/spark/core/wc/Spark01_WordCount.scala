package com.ustb.ly.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和Spark框架的连接
    //JDBC：Connection
    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //1.读取文件，获取一行一行的数据
    //Hello World
    val lines: RDD[String] = sc.textFile("datas")

    //2.将一行数据进行拆分，形成一个个的单词（分词=>扁平化：将整体拆分成个体的操作）
    //"Hello World" => Hello,World...
    //val words: RDD[String] = lines.flatMap(s => s.split(" "))
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组，便于统计
    //(hello,hello...),(world,world,world...)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //4.将分组后的数据进行转换
    //(hello,hello...),(world,world,world...)
    //(hello,2),(world,3)
    val res: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5.将转换的结果采集到控制台打印出来
    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }
}
