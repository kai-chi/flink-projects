package kaichi.flink.exercises

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object MathExample {
  def main(args: Array[String]): Unit = {
    val input = ParameterTool.fromArgs(args).getRequired("input")

    val maxDelay = 60
    val servingSpeed = 100

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fares = env.addSource(
      new TaxiFareSource(input, maxDelay, servingSpeed))

    val calculations = fares
      .map(f => f.totalFare).setParallelism(1)
      .map(f => f * f).setParallelism(6)
      .map(f => f + 5).setParallelism(2)
      .filter(_.toInt % 2 == 0).setParallelism(5)
      .filter(_.toInt + 10 > 30).setParallelism(3)
      .filter(_.toInt < 500).setParallelism(4)


    calculations.print()

    env.execute("Math Example")
  }

}
