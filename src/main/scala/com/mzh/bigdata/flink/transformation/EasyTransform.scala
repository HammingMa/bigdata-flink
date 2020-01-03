package com.mzh.bigdata.flink.transformation

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

case class SensorReading(id: String, time: String, temperature: Double)

object EasyTransform {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sensorDS: DataStream[SensorReading] = fileDS.map(line => {
      val feilds: Array[String] = line.split(",")
      SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble)
    })

    //val keyDS: KeyedStream[SensorReading, Tuple] = sensorDS.keyBy(0)
    //val sensorSumDS: DataStream[SensorReading] = keyDS.sum(2)

    //val keyDS: KeyedStream[SensorReading, Tuple] = sensorDS.keyBy("id")
    //val sensorSumDS: DataStream[SensorReading] = keyDS.sum("temperature")

    val keyDS: KeyedStream[SensorReading, String] = sensorDS.keyBy(_.id)
    val sensorSumDS: DataStream[SensorReading] = keyDS.sum(2)

    sensorSumDS.print("sensor")

    env.execute("essy transform")

  }
}
