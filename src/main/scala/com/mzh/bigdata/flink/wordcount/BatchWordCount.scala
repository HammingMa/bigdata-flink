package com.mzh.bigdata.flink.wordcount

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val filePath: String = "./input/words.txt"

    val fileDS: DataSet[String] = env.readTextFile(filePath)

    val word2CountDS: AggregateDataSet[(String, Int)] = fileDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    word2CountDS.print()

  }
}
