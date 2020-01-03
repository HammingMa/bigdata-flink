package com.mzh.bigdata.flink.sink



import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._


object JDBCSink {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

        val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

        val sensorDS: DataStream[SensorReading] = fileDS.map(
          data => {
            val feilds: Array[String] = data.split(",")
            SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble)
          }
        )

    sensorDS.addSink(new MyJdbcSink())
    env.execute("kafka sink")
  }
}

class MyJdbcSink extends RichSinkFunction[SensorReading]{
  var conn:Connection =_
  var insertStmt:PreparedStatement=_
  var updateStmt:PreparedStatement=_

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    conn = DriverManager.getConnection("jdbc:mysql://hdp3:3306/commerce","root","root")

    insertStmt = conn.prepareStatement("insert into sensor (id,time,temperature) values (?,?,?) ")

    updateStmt = conn.prepareStatement("update sensor set time=?, temperature= ? where id =?")

  }

  override def invoke(sensor: SensorReading, context: SinkFunction.Context[_]): Unit = {

    println(sensor.temperature)

    updateStmt.setString(1, sensor.time)
    updateStmt.setDouble(2, sensor.temperature)
    updateStmt.setString(3, sensor.id)

    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, sensor.id)
      insertStmt.setString(2, sensor.time)
      insertStmt.setDouble(3, sensor.temperature)

      insertStmt.execute()
    }


  }

  override def close(): Unit = {
    super.close()

    updateStmt.close()
    insertStmt.close()

    conn.close()

  }
}

