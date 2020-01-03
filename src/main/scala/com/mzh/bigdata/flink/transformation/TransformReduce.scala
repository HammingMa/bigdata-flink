package com.mzh.bigdata.flink.transformation

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object TransformReduce {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sensorDS: DataStream[SensorReading] = fileDS.map(line => {
      val feilds: Array[String] = line.split(",")
      SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble)
    })

    //取上一个时间减一，当前温度加10

    val keyDS: KeyedStream[SensorReading, String] = sensorDS.keyBy(_.id)

    val reduceDS: DataStream[SensorReading] = keyDS.reduce((x,y)=>SensorReading(y.id,(x.time.toLong-1).toString,y.temperature+10))

    reduceDS.print("sensor")

    env.execute("essy transform")

  }
}
