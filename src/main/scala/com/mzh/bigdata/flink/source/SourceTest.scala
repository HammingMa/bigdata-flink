package com.mzh.bigdata.flink.source

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable

case class SensorReading(id: String, time: String, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val SendsorDS: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("1", "154771653", 29.23259),
      SensorReading("2", "154733222", 27.23259),
      SensorReading("3", "154516789", 25.23259),
      SensorReading("4", "154856343", 23.23259)
    ))

    // SendsorDS.print("Sendsor1").setParallelism(1)

    val fileSource: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")
    // fileSource.print("file1")


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092")
    properties.setProperty("group.id", "flinksource")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //kafkaSource.print("kafka source")

    val mySource: DataStream[SensorReading] = env.addSource(new SensorSource())


    //mySource.print("mysource")

    val eleDS: DataStream[Long] = env.fromElements(1L,2l,3L,4L,5L)
    eleDS.print()

    env.execute("source test")

  }
}

class SensorSource extends SourceFunction[SensorReading] {

  private var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random: Random = new Random()

    val ids: immutable.IndexedSeq[(Int, Double)] = (1 to 10).map(
      i => (i, 60 + random.nextGaussian() * 20)
    )
    while (running) {
      val sedsors: immutable.IndexedSeq[SensorReading] = ids.map(
        item => SensorReading(item._1.toString, System.currentTimeMillis().toString, item._2 + random.nextGaussian() * 20)
      )

      sedsors.foreach(
        sedsor => sourceContext.collect(sedsor)
      )

      Thread.sleep(500)
    }


  }

  override def cancel(): Unit = {
    running = false
  }
}
