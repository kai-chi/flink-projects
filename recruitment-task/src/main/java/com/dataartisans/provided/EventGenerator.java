package com.dataartisans.provided;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * NOTE: Everything in the "provided" package IS NOT representative for
 * the level of quality we are expecting for the coding task submission.
 */
public class EventGenerator implements ParallelSourceFunction<Event>, ListCheckpointed<Long> {

    private static final long serialVersionUID = 9159457797902106334L;
    public static final long OutOfOrderness = 15000L;

    private boolean running = true;

    public long counter = OutOfOrderness;

    public void run(SourceContext<Event> sourceContext) throws Exception {
        StringBuffer buf = new StringBuffer();
        while (running) {
            Event evt = new Event();

            evt.time = counter + ThreadLocalRandom.current().nextLong(-OutOfOrderness, OutOfOrderness);
            buf.append("original index:").append(counter).append(" time: ").append(evt.time);
            evt.someData = buf.toString();
            buf.setLength(0);

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(evt);
                counter++;
            }

            if (counter % 2L == 0) { //1_000L
                Thread.sleep(20); // 10
            }
        }
    }

    public void cancel() {
        running = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(counter);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        counter = state.get(0);
    }
}
