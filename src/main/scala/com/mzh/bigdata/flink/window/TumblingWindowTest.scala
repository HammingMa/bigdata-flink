package com.mzh.bigdata.flink.window


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindowTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //设置Watermark的以事件时间为准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置watermark生成的时间 默认 200毫秒
    env.getConfig.setAutoWatermarkInterval(100L)

    //val fileDS: DataStream[String] = env.readTextFile("./input/SendsorReading.txt")

    val sorcketDS: DataStream[String] = env.socketTextStream("localhost", 7777)



    val sensorDS: DataStream[SensorReading] = sorcketDS.filter(!_.isEmpty).map(
      data => {
        val feilds: Array[String] = data.split(",")
        SensorReading(feilds(0).trim, feilds(1).trim.toLong, feilds(2).trim.toDouble)
      }
    )
      //设置事件时间取哪个字段，只有事件时间没有延迟时间
      //.assignAscendingTimestamps(_.time*1000)
      //设置自定义周期watermark生成方式
      //.assignTimestampsAndWatermarks(new MyAssigner())
      //设置watermark的生成方式和字段
      //.assignTimestampsAndWatermarks(new MyAssigner2)
      //系统提供的Watermark的生成方式类，以sensor.time 标准 延迟一秒生成水位的方式
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.time * 1000
    })


    val minDS: DataStream[(String, Double)] = sensorDS.map(sensor => (sensor.id, sensor.temperature))
      .keyBy(_._1)
      //滚动窗口
      .timeWindow(Time.seconds(5))
      //滑动窗口
      //.timeWindow(Time.seconds(8),Time.seconds(4))
      //用底层window实现滑动窗口
      //.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5),Time.hours(8)))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    minDS.print("min temperature")

    sensorDS.print("sensor")

    env.execute()

  }

}

//自定义的周期周期生成的watermark
class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {

  //延迟时间
  private val bound = 6000
  //当前最大时间
  private var maxTS: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {

    new Watermark(maxTS - bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    //和最大时间比最大时间
    maxTS = maxTS.max(t.time * 1000)
    //设置要取的时间字段
    t.time * 1000
  }
}

//设置自定义生成watermark的方式
class MyAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(t: SensorReading, extracedTimestamp: Long): Watermark = {
    //来一条数据生成一个watermark
    new Watermark(extracedTimestamp)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.time * 1000
  }
}
