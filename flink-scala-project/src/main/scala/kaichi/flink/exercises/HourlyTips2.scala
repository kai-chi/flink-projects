package kaichi.flink.exercises

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Solution incomplete - does not return window's end timestamp
  */
object HourlyTips2 {
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
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .aggregate(new TipAggregate())

    val bestDriver = tipsPerHour
      .timeWindowAll(Time.hours(1))
      .maxBy(1)

    bestDriver.print()

    env.execute("Taxi Fare Hourly Tips")
  }

  class TipAggregate extends AggregateFunction[TaxiFare, (Long, Float), (Long, Float)] {

    override def add(value: TaxiFare, accumulator: (Long, Float)): (Long, Float) = (value.driverId, accumulator._2 + value.tip)

    override def createAccumulator(): (Long, Float) = (0L, 0f)

    override def getResult(accumulator: (Long, Float)): (Long, Float) = accumulator

    override def merge(a: (Long, Float), b: (Long, Float)): (Long, Float) = (a._1, a._2 + b._2)
  }

}
