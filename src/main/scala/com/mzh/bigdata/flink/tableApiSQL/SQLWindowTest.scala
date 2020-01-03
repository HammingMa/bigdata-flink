package com.mzh.bigdata.flink.tableApiSQL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

case class SensorReading1(id: String, times: Long, temperature: Double)

object SQLWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    env.setParallelism(1)

    //设置watermark以事件事件为准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sensorDS: DataStream[SensorReading1] = fileDS.map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading1(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading1](Time.seconds(2)) {
      //设置延迟时间2秒 以time属性做事件事件
      override def extractTimestamp(t: SensorReading1): Long = t.times*1000
    })



    val sensorTable: Table = tableEnv.fromDataStream(sensorDS,'id,'times.rowtime,'temperature)

    val sql: String = "select " +
      "id, " +
      "count(id) as cnt " +
      "from "+sensorTable +
      " group by id,tumble(times,interval '10' second) "

    val sqlTable: Table = tableEnv.sqlQuery(sql)



    val ds: DataStream[(Boolean, (String, Long))] = sqlTable.toRetractStream[(String,Long)]



    ds.print()

    env.execute()

  }
}
