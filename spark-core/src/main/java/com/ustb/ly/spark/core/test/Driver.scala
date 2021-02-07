package com.ustb.ly.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    //客户端：Client 发送数据
    val socket1 = new Socket("localhost",9999)
    val socket2 = new Socket("localhost",8888)

    val task = new Task

    val out1: OutputStream = socket1.getOutputStream
    val objectOutputStream1 = new ObjectOutputStream(out1)

    val subTask = new SubTask
    subTask.logic = task.logic
    subTask.data = task.data.take(2)

    objectOutputStream1.writeObject(subTask)

    objectOutputStream1.flush()
    objectOutputStream1.close()
    socket1.close()


    val out2: OutputStream = socket2.getOutputStream
    val objectOutputStream2 = new ObjectOutputStream(out2)

    val subTask1 = new SubTask
    subTask1.logic = task.logic
    subTask1.data = task.data.takeRight(2)

    objectOutputStream2.writeObject(subTask1)

    objectOutputStream2.flush()
    objectOutputStream2.close()
    socket2.close()
  }
}
