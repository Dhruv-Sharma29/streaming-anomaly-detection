package pipeline;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pipeline.model.Event;

/**
 * Makes every late-drop decision with a watermark kept in checkpointed operator state, using the
 * reference's exact rule: watermark = (max event time before this event) - bound - 1 ms.
 *
 * Why not Flink's window-operator lateness: Flink does not checkpoint watermarks. After a restore
 * the window operator's watermark restarts at Long.MIN_VALUE, so a very-late event replayed first
 * re-creates its long-purged window and the upsert sink overwrites the correct row with a 1-event
 * aggregate (reproduced: eval.run_pipeline --crash job --before-very-late). This operator's
 * watermark is restored with the source offsets, so decisions are identical before and after
 * recovery. Flink's own watermark is never ahead of it, so the window operator never drops an
 * event this filter forwards.
 *
 * Runs at parallelism 1 directly after the single-partition source: arrival order is preserved.
 */
public class LatenessFilter extends ProcessFunction<Event, Event> implements CheckpointedFunction {
    private final long boundMs, windowMs, latenessMs;
    private final String flushService;
    private final OutputTag<Event> lateTag;
    private long maxTs = Long.MIN_VALUE;
    private transient ListState<Long> checkpointed;

    public LatenessFilter(Constants k, OutputTag<Event> lateTag) {
        this.boundMs = k.boundMs;
        this.windowMs = k.windowMs;
        this.latenessMs = k.latenessMs;
        this.flushService = k.flushService;
        this.lateTag = lateTag;
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Event> out) {
        boolean first = maxTs == Long.MIN_VALUE;
        long watermark = maxTs - boundMs - 1;        // before this event, as in the reference
        maxTs = Math.max(maxTs, e.tsMs);
        if (flushService.equals(e.service)) return;  // flush markers only advance time
        long windowMax = e.tsMs - Math.floorMod(e.tsMs, windowMs) + windowMs - 1;
        if (!first && windowMax + latenessMs - watermark <= 0) ctx.output(lateTag, e);
        else out.collect(e);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointed.update(java.util.List.of(maxTs));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointed = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("max-ts", Long.class));
        for (Long v : checkpointed.get()) maxTs = Math.max(maxTs, v);
    }
}
