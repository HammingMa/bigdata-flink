package com.mzh.bigdata.flink.transformation

import org.apache.flink.streaming.api.scala._


object ConnectAndCoMap {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sensorDS: DataStream[SensorReading] = fileDS.map(line => {
      val feilds: Array[String] = line.split(",")
      SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble)
    })


    val splitStream: SplitStream[SensorReading] = sensorDS.split(sensor => if(sensor.temperature>25) Seq("high") else Seq("lower"))

    val highDS: DataStream[SensorReading] = splitStream.select("high")
    val lowerDS: DataStream[SensorReading] = splitStream.select("lower")
    val allDS: DataStream[SensorReading] = splitStream.select("high","lower")


    val highAndLowerDS: ConnectedStreams[SensorReading, SensorReading] = highDS.connect(lowerDS)
    val coMapDS: DataStream[Product] = highAndLowerDS.map(data =>(data.id,data.temperature,"warming"),data=>(data.id,data.temperature))


    coMapDS.print()

    env.execute("ConnectAndCoMap")

  }
}
