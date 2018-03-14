package kaichi.flink.exercises;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TaxiRideCleansingWeights {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");
        final int maxDelay = 60;
        final int maxSpeed = 60;

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(input, maxDelay, maxSpeed));

        DataStream<TaxiRide> filteredRides = rides
                .filter(new NYCFilter());

        DataStream<TaxiRideWeights> weightedRides = filteredRides
                .map(TaxiRideWeights::new);

        DataStream<TaxiRideWeights> weightedFilteredRides = weightedRides
                .filter(new WeightFilter());

        weightedFilteredRides.print();

        env.execute("Taxi Ride Cleansing");
    }

    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

    public static class WeightFilter implements FilterFunction<TaxiRideWeights> {

        @Override
        public boolean filter(TaxiRideWeights taxiRideWeights) throws Exception {
            return taxiRideWeights.getWeight() > 0.5;
        }
    }
}
