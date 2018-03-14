package kaichi.flink.exercises;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;

import java.util.concurrent.ThreadLocalRandom;

public class TaxiRideWeights extends TaxiRide implements Weightable {

    private final double weight;

    public TaxiRideWeights(TaxiRide t) {

        super(t.rideId, t.isStart, t.startTime, t.endTime, t.startLon, t.startLat,
                t.endLon, t.endLat, t.passengerCnt, t.taxiId, t.driverId);
        this.weight = ThreadLocalRandom.current().nextDouble();
    }

    @Override
    public double getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return super.toString().concat(",").concat(String.format("%.2f", weight));
    }
}
