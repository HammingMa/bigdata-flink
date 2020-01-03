package com.mzh.bigdata.flink.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSink {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")
    //
    //    val sensorDS: DataStream[String] = fileDS.map(
    //      data => {
    //        val feilds: Array[String] = data.split(",")
    //        SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble).toString
    //      }
    //    )

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092")
    properties.setProperty("group.id", "flinksource")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val sensorDS: DataStream[String] = kafkaSource.map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble).toString
      })


    sensorDS.addSink(new FlinkKafkaProducer011[String]("hdp1:9092,hdp2:9092,hdp3:9092", "sinkTest", new SimpleStringSchema()))

    sensorDS.print()

    env.execute("kafka sink")
  }
}

case class SensorReading(id: String, time: String, temperature: Double)
