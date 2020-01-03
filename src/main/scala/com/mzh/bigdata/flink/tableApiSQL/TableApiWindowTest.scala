package com.mzh.bigdata.flink.tableApiSQL

import com.mzh.bigdata.flink.processfunc.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

object TableApiWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    env.setParallelism(1)

    //设置watermark以事件事件为准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sensorDS: DataStream[SensorReading] = fileDS.map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
      //设置延迟时间2秒 以time属性做事件事件
      override def extractTimestamp(t: SensorReading): Long = t.time*1000
    })



    val sensorTable: Table = tableEnv.fromDataStream(sensorDS,'id,'time.rowtime,'temperature)


    val windowTable: Table = sensorTable.window(Tumble over 10000.millis on 'time as 'tt)
      .groupBy('id, 'tt)
      .select('id, 'id.count as 'cnt)




    val ds: DataStream[(Boolean, (String, Long))] = windowTable.toRetractStream[(String,Long)]



    ds.print()

    env.execute()

  }
}
