package com.mzh.bigdata.flink.state

import com.mzh.bigdata.flink.processfunc.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ValueStateTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDS: DataStream[SensorReading] = socketDS.filter(!_.isEmpty).map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    )

    sensorDS.keyBy(_.id)
      .process(new TemperatureAlterGreatProcess(5.0))
      .print("process")

    sensorDS.keyBy(_.id)
      .flatMap(new TemperatureAlterGreatFlatMap(10.0))
      .print("flatmap")

    sensorDS.print("sensorDS")

    env.execute()

  }

}

class TemperatureAlterGreatProcess(warningTemp: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  private lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(sensor: SensorReading,
                              context: KeyedProcessFunction[String,
                                SensorReading, (String, Double, Double)]#Context,
                              collector: Collector[(String, Double, Double)]): Unit = {
    val lastTemp: Double = lastTempState.value()

    if (lastTemp == 0.0) {
      lastTempState.update(sensor.temperature)
      return
    }

    val diffTemp: Double = (lastTemp - sensor.temperature).abs

    if (diffTemp > warningTemp) {
      collector.collect((sensor.id, lastTemp, sensor.temperature))
    }

    lastTempState.update(sensor.temperature)
  }
}

class TemperatureAlterGreatFlatMap(warningTemp: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _


  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp2", classOf[Double]))
  }

  override def flatMap(sensor: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    val lastTemp: Double = lastTempState.value()

    if (lastTemp == 0.0) {
      lastTempState.update(sensor.temperature)
      return
    }

    val diffTemp: Double = (lastTemp - sensor.temperature).abs

    if (diffTemp > warningTemp) {
      collector.collect((sensor.id, lastTemp, sensor.temperature))
    }

    lastTempState.update(sensor.temperature)
  }
}
