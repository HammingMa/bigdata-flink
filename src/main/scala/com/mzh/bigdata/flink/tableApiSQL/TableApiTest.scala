package com.mzh.bigdata.flink.tableApiSQL

import com.mzh.bigdata.flink.processfunc.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._

object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    env.setParallelism(1)

    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sensorDS: DataStream[SensorReading] = fileDS.map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    )

    val sensorTable: Table = tableEnv.fromDataStream(sensorDS)

    val selectTable: Table = sensorTable.select("id,temperature").filter("temperature>10")


    val ds: DataStream[(String, Double)] = selectTable.toAppendStream[(String,Double)]

    ds.print()

    env.execute()

  }
}
