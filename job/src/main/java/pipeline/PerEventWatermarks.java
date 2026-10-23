package pipeline;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import pipeline.model.Envelope;

/**
 * Bounded-out-of-orderness watermarks emitted after every event (not every 200 ms of processing
 * time). With a single input partition, the watermark seen with each element is then exactly the
 * reference's (max event time before it - bound - 1 ms), apart from network-buffer delay. The
 * periodic default lagged up to ~9 s of event time at 46x replay speed, enough to change
 * late-event decisions (docs/semantics.md, "Determinism and replay speed").
 */
public class PerEventWatermarks implements WatermarkGenerator<Envelope> {
    private final long boundMs;
    private long maxTs = Long.MIN_VALUE;
    private long emitted = Long.MIN_VALUE;

    public PerEventWatermarks(long boundMs) { this.boundMs = boundMs; }

    @Override
    public void onEvent(Envelope e, long eventTimestamp, WatermarkOutput output) {
        if (e.event == null) return;  // invalid records carry no event time
        maxTs = Math.max(maxTs, eventTimestamp);
        emitIfAdvanced(output);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) { emitIfAdvanced(output); }

    private void emitIfAdvanced(WatermarkOutput output) {
        if (maxTs == Long.MIN_VALUE) return;
        long wm = maxTs - boundMs - 1;
        if (wm > emitted) {
            emitted = wm;
            output.emitWatermark(new Watermark(wm));
        }
    }
}
