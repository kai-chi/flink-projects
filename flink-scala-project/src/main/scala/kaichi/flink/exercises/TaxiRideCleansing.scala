package kaichi.flink.exercises

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object TaxiRideCleansing {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val maxDelay = 60
    val servingSpeed = 6

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed))

    val nycRides = rides
      .filter(x => GeoUtils.isInNYC(x.startLon, x.startLat) && GeoUtils.isInNYC(x.endLon, x.endLat))

    nycRides.print()

    env.execute("Taxi Ride NYC Filtering")
  }

}
