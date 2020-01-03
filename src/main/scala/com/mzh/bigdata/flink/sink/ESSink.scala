package com.mzh.bigdata.flink.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ESSink {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

        val sensorDS: DataStream[SensorReading] = fileDS.map(
          data => {
            val feilds: Array[String] = data.split(",")
            SensorReading(feilds(0).trim, feilds(1).trim, feilds(2).trim.toDouble)
          }
        )

    val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()

    hosts.add(new HttpHost("hdp2",9200))

    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](hosts, new ElasticsearchSinkFunction[SensorReading] {
      override def process(sensor: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("save data :" + sensor)

        val json: util.HashMap[String, String] = new util.HashMap[String, String]
        json.put("id", sensor.id)
        json.put("time", sensor.time)
        json.put("temperature", sensor.temperature.toString)

        val request: IndexRequest = Requests.indexRequest().index("sensor").`type`("_doc").source(json)

        requestIndexer.add(request)

        println("saved successfully")


      }
    })



    sensorDS.addSink(esSinkBuilder.build())


    env.execute("kafka sink")
  }
}
