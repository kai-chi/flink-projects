package com.dataartisans;

import com.dataartisans.provided.Event;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class OrderOperatorTest {

    private static final int SIZE = 1000000;
    private static final long OUT_OF_ORDERNESS = 15000L;

    @Test
    public void testOrderOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        List<Event> list = getRandomEventList(SIZE);
        CollectSink.values.clear();


        DataStream<Event> eventStream = env.fromCollection(list);
        eventStream.assignTimestampsAndWatermarks(new EventAssigner())
                .transform("OrderOperator", eventStream.getType(), new OrderOperator<>())
                .addSink(new CollectSink());
        env.execute();


        //make sure no records are lost
        assertEquals("Records dropped or multiplied", SIZE, CollectSink.values.size());
        //make sure the records are in order
        long timestamp = -OUT_OF_ORDERNESS;
        for (int i = 0; i < CollectSink.values.size(); i++) {
            Event e = CollectSink.values.get(i);
            assertTrue("Collected elements not in order", timestamp <= e.time);
            timestamp = e.time;
        }
    }

    private List<Event> getRandomEventList(int size) {
        List<Event> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            long timestamp = i + ThreadLocalRandom.current().nextLong(-OUT_OF_ORDERNESS, OUT_OF_ORDERNESS);
            list.add(new Event(timestamp, String.valueOf(timestamp)));
        }
        return list;
    }

    private static class CollectSink implements SinkFunction<Event> {

        public static final List<Event> values = new ArrayList<>();

        @Override
        public void invoke(Event value) throws Exception {
            values.add(value);
        }
    }
}
