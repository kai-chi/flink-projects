package kaichi.flink.exercises;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RideEnrichment {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesInput = params.getRequired("rides");
        final String faresInput = params.getRequired("fares");
        final int maxDelay = 60;
        final int maxSpeed = 100;

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env
                .addSource(new TaxiRideSource(ridesInput, maxDelay, maxSpeed))
                .filter(taxiRide -> !taxiRide.isStart)
                .keyBy("rideId");

        DataStream<TaxiFare> fares = env
                .addSource(new TaxiFareSource(faresInput, maxDelay, maxSpeed))
                .keyBy("rideId");


        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
                .connect(fares)
                .flatMap(new EnrichRidesWithFares());

        enrichedRides.print();

        env.execute("Taxi Ride Cleansing");
    }

    private static class EnrichRidesWithFares extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration parameters) throws Exception {
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiFare>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(new Tuple2<>(ride, fare));
            } else {
                rideState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(new Tuple2<>(ride, fare));
            } else {
                fareState.update(fare);
            }
        }
    }
}
