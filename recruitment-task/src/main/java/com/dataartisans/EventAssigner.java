package com.dataartisans;

import com.dataartisans.provided.Event;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class EventAssigner implements AssignerWithPeriodicWatermarks<Event> {
    private static final long serialVersionUID = -7789161124369580380L;
    private static final long OUT_OF_ORDERNESS = 15000L;
    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        // the borderline case is when the currentMaxTimestamp = (timestamp + OUT_OF_ORDERNESS)
        // and the next Event will arrive with timestamp = (timestamp + 1 - OUT_OF_ORDERNESS)
        return new Watermark(currentMaxTimestamp - 2 * OUT_OF_ORDERNESS);
    }

    @Override
    public long extractTimestamp(Event event, long l) {
        long timestamp = event.time;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
