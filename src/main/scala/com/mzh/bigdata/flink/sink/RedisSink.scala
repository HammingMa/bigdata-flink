package com.mzh.bigdata.flink.sink



import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

        val sensorDS: DataStream[SensorReading] = fileDS.map(
          data => {
            val feilds: Array[String] = data.split(",")
            SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble)
          }
        )

    val config: FlinkJedisPoolConfig.Builder = new FlinkJedisPoolConfig.Builder

    config.setHost("hdp2")
    config.setPort(6379)

    sensorDS.addSink(new RedisSink[SensorReading](config.build(),new MyRedisMapper()))

    sensorDS.print()

    env.execute("redis sink")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}

