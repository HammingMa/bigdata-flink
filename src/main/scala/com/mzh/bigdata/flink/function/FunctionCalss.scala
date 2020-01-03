package com.mzh.bigdata.flink.function

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FunctionCalss {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val listDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6))

    val myFilterDS: DataStream[Int] = listDS.filter(new MyFilter())
    myFilterDS.print("My Filter")

    env.execute("My Filter")
  }
}

class MyFilter extends FilterFunction[Int] {
  override def filter(t: Int): Boolean = {
    t % 2 == 0
  }
}
