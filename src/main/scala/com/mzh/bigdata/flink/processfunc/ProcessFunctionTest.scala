package com.mzh.bigdata.flink.processfunc

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(6000L)

//    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend(""))
//    env.setStateBackend(new RocksDBStateBackend(""))

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDS: DataStream[SensorReading] = socketDS.filter(!_.isEmpty).map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    )

    val processDS: DataStream[SensorReading] = sensorDS.process(new FreeingMonitor())

    processDS.print("process")

    processDS.getSideOutput(new OutputTag[String]("lower")).print("side")

    env.execute()

  }
}

class FreeingMonitor extends ProcessFunction [SensorReading,SensorReading]{
  override def processElement(sensor: SensorReading,
                              context: ProcessFunction[SensorReading, SensorReading]#Context,
                              collector: Collector[SensorReading]): Unit = {


    if(sensor.temperature<10){
      val outputTag: OutputTag[String] = new OutputTag[String]("lower")

      context.output(outputTag,sensor.id)

    }else{
      collector.collect(sensor)
    }
  }
}
