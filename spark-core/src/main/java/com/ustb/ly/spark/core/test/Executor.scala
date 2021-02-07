package com.ustb.ly.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {
    //服务器端：Server 接收数据
    val server = new ServerSocket(9999)
    println("服务启动成功，准备接受数据...")

    val socket: Socket = server.accept()
    val in: InputStream = socket.getInputStream
    val objectInputStream = new ObjectInputStream(in)

    val task: SubTask = objectInputStream.readObject().asInstanceOf[SubTask]
    val list: List[Int] = task.compute()
    println("9999读取到的数据为：" + list)

    objectInputStream.close()
    socket.close()
    server.close()
  }
}
