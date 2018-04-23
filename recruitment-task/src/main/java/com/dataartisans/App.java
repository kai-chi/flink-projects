package com.dataartisans;

import com.dataartisans.provided.Event;
import com.dataartisans.provided.EventGenerator;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));

        DataStream<Event> eventStream = env.addSource(new EventGenerator());
        eventStream = eventStream
                .assignTimestampsAndWatermarks(new EventAssigner())
                .transform("OrderOperator", eventStream.getType(), new OrderOperator<>()).setParallelism(2);

        eventStream.print();

        env.execute("Run Stream smoother");
    }
}
