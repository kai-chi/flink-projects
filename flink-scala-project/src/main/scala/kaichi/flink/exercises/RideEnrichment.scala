package kaichi.flink.exercises

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{TaxiFareSource, TaxiRideSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RideEnrichment {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val inputFares = params.getRequired("fares")
    val inputRides = params.getRequired("rides")

    val maxDelay = 60
    val servingSpeed = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides = env
      .addSource(new TaxiRideSource(inputRides, maxDelay, servingSpeed))
      .filter(ride => !ride.isStart)
      .keyBy("rideId")

    val fares = env
      .addSource(new TaxiFareSource(inputFares, maxDelay, servingSpeed))
      .keyBy("rideId")

    val enrichedRides = rides
      .connect(fares)
      .flatMap(new EnrichRides)
      .print()

    env.execute("Ride Enrichment")
  }

  class EnrichRides extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        out.collect((ride, fare))
      }
      else {
        rideState.update(ride)
      }
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear
        out.collect((ride, fare))
      }
      else {
        fareState.update(fare)
      }
    }
  }

}
