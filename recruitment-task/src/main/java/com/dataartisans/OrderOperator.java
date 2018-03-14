package com.dataartisans;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Comparator;
import java.util.PriorityQueue;

public class OrderOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, CheckpointedFunction {

    private static final long serialVersionUID = 607406801052018312L;

    private PriorityQueue<StreamRecord<T>> queue = new PriorityQueue<>(new StreamRecordComparator());
    private transient ValueState<PriorityQueue<StreamRecord<T>>> checkpointedState;

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        queue.add(streamRecord);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        StreamRecord<T> head = queue.peek();
        while (head != null && head.getTimestamp() <= mark.getTimestamp()) {
            output.collect(head);
            queue.remove(head);
            head = queue.peek();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.update(queue);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ValueStateDescriptor<PriorityQueue<StreamRecord<T>>> valueStateDescriptor = new ValueStateDescriptor<>(
                "queued-events",
                TypeInformation.of(new TypeHint<PriorityQueue<StreamRecord<T>>>() {
                }));
        checkpointedState = context.getKeyedStateStore().getState(valueStateDescriptor);

        if (context.isRestored()) {
            PriorityQueue<StreamRecord<T>> pq = checkpointedState.value();
            StreamRecord<T> head = pq.peek();
            while (head != null) {
                output.collect(head);
                pq.remove(head);
                head = pq.peek();
            }
        }
    }

    private class StreamRecordComparator implements Comparator<StreamRecord>, Serializable {
        private static final long serialVersionUID = 4021677307733695199L;

        @Override
        public int compare(StreamRecord streamRecord, StreamRecord t1) {
            return Long.compare(streamRecord.getTimestamp(), t1.getTimestamp());
        }
    }
}
