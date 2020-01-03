package com.mzh.bigdata.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val parameter: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = parameter.get("host")
    val port: Int = parameter.getInt("port")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //关闭任务链优化
    //env.disableOperatorChaining()

    val socketDS: DataStream[String] = env.socketTextStream(host,port)

    val word2CountDS: DataStream[(String, Int)] = socketDS.filter(!_.isEmpty)
      .flatMap(_.split(" ")).disableChaining().startNewChain()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    word2CountDS.print()

    env.execute("Stream word count ")


  }
}
