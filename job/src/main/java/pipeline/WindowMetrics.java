package pipeline;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pipeline.model.Event;
import pipeline.model.WindowResult;

import java.util.Arrays;

/** Window metrics as defined in docs/semantics.md; marks first firing vs late-event update. */
public class WindowMetrics extends ProcessWindowFunction<Event, WindowResult, String, TimeWindow> {
    private static final ValueStateDescriptor<Boolean> FIRED = new ValueStateDescriptor<>("fired", Boolean.class);
    private final int errorStatusMin;

    public WindowMetrics(int errorStatusMin) { this.errorStatusMin = errorStatusMin; }

    @Override
    public void process(String service, Context ctx, Iterable<Event> events, Collector<WindowResult> out) throws Exception {
        int count = 0, errors = 0, queueMax = Integer.MIN_VALUE;
        double[] lat = new double[16];
        for (Event e : events) {
            if (count == lat.length) lat = Arrays.copyOf(lat, count * 2);
            lat[count++] = e.latencyMs;
            if (e.status >= errorStatusMin) errors++;
            queueMax = Math.max(queueMax, e.queueDepth);
        }
        double[] sorted = Arrays.copyOf(lat, count);
        Arrays.sort(sorted);
        WindowResult r = new WindowResult();
        r.service = service;
        r.windowStartMs = ctx.window().getStart();
        r.requestCount = count;
        r.errorCount = errors;
        r.latencyP95Ms = sorted[Math.max(0, (int) Math.ceil(0.95 * count) - 1)];  // nearest rank, as in Python
        r.queueDepthMax = queueMax;
        ValueState<Boolean> fired = ctx.windowState().getState(FIRED);
        r.isUpdate = fired.value() != null;
        fired.update(true);
        out.collect(r);
    }

    @Override
    public void clear(Context ctx) {
        ctx.windowState().getState(FIRED).clear();
    }
}
