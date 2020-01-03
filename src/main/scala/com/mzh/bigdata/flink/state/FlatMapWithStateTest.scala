package com.mzh.bigdata.flink.state

import com.mzh.bigdata.flink.processfunc.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

object FlatMapWithStateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)


    //开启checkpoint
    env.enableCheckpointing(60000)
    //设置检查点的模式是精确一次还是至少一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //checkpoint的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(6000)
    //设置检查点失败是否任务失败
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //设置最多同时可以有几个checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //checkpoint最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    //设置任务失败或者手动取消是否删除checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    //重启策略重启3次每次间隔500毫秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))
    //设置重启策略 300秒内重启3次每次重启的时间间隔40秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(300),Time.seconds(40)))


    val socketDS: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDS: DataStream[SensorReading] = socketDS.filter(!_.isEmpty).map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    )

    val flatMapWithStateDS: DataStream[(String, Double, Double)] = sensorDS.keyBy(_.id)
      .flatMapWithState [(String,Double,Double),Double]{
        case (sensor: SensorReading, None) => (List.empty, Some(sensor.temperature))
        case (sensor: SensorReading, lastTemp: Some[Double]) =>
          val diffTemp: Double = (sensor.temperature - lastTemp.get).abs
          if (diffTemp > 10.0) {
            (List((sensor.id, lastTemp.get, sensor.temperature)), Some(sensor.temperature))
          } else {
            (List.empty, Some(sensor.temperature))
          }
      }

    sensorDS.print("sensorDS")

    flatMapWithStateDS.print("flatMapWithStateDS")

    env.execute()
  }
}
