package com.mzh.bigdata.flink.processfunc

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDS: DataStream[SensorReading] = socketDS.filter(!_.isEmpty).map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    )


    val processDS: DataStream[String] = sensorDS.keyBy(_.id)
      .process(new MyProcess())

    processDS.print("process")

    sensorDS.print("sensor")

    env.execute()

  }
}

class MyProcess extends KeyedProcessFunction[String, SensorReading, String] {

  private lazy val lastTemperature: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temperature", classOf[Double]))

  private lazy val lastTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last_timer",classOf[Long]))

  override def processElement(sensor: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

    val lastTemp: Double = lastTemperature.value()
    val currentTemp: Double = sensor.temperature

    val lastTimerTS: Long = lastTimer.value()

    lastTemperature.update(currentTemp)

    if (currentTemp > lastTemp && lastTimerTS == 0 && lastTemp != 0.0) {
      val timerTS: Long = context.timerService().currentProcessingTime() + 5000L
      context.timerService().registerProcessingTimeTimer(timerTS)
      lastTimer.update(timerTS)
    } else if (currentTemp < lastTemp || lastTemp == 0.0) {

      context.timerService().deleteProcessingTimeTimer(lastTimerTS)
      lastTimer.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey+"温度连续上升")

    lastTimer.clear()
  }
}
