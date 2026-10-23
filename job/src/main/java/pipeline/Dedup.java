package pipeline;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pipeline.model.Event;

/**
 * Keyed by event_id. Drops an event whose id was seen until the watermark passes first_ts + H,
 * when an event-time timer clears the state (docs/semantics.md, De-duplication).
 */
public class Dedup extends KeyedProcessFunction<String, Event, Event> {
    private final long horizonMs;
    private transient ValueState<Long> firstTs;

    public Dedup(long horizonMs) { this.horizonMs = horizonMs; }

    @Override
    public void open(OpenContext ctx) {
        firstTs = getRuntimeContext().getState(new ValueStateDescriptor<>("first-ts", Long.class));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Event> out) throws Exception {
        if (firstTs.value() != null) return;  // duplicate within the horizon
        firstTs.update(e.tsMs);
        ctx.timerService().registerEventTimeTimer(e.tsMs + horizonMs);
        out.collect(e);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) {
        firstTs.clear();
    }
}
