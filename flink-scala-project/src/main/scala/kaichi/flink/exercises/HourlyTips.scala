package kaichi.flink.exercises

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object HourlyTips {
  def main(args: Array[String]): Unit = {
    val input = ParameterTool.fromArgs(args).getRequired("input")

    val maxDelay = 60
    val servingSpeed = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fares = env.addSource(
      new TaxiFareSource(input, maxDelay, servingSpeed))

    val tipsPerHour = fares
      .keyBy(f => f.driverId)
      .timeWindow(Time.hours(1))
      .apply { (key: Long, window, fares, out: Collector[(Long, Long, Float)]) =>
        out.collect((window.getEnd, key, fares.map(fare => fare.tip).sum))

      }

    val bestDriver = tipsPerHour
      .timeWindowAll(Time.hours(1))
      .maxBy(2)

    bestDriver.print()

    env.execute("Taxi Fare Hourly Tips")
  }

}
