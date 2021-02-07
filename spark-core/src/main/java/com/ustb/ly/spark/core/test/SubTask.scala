package com.ustb.ly.spark.core.test

class SubTask extends Serializable {
  //数据
  var data:List[Int] = _

  //逻辑
  var logic: (Int => Int) = _

  //计算
  def compute() = {
    data.map(logic)
  }
}
