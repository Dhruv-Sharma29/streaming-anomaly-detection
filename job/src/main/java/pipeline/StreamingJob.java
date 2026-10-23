package pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pipeline.model.Alert;
import pipeline.model.Envelope;
import pipeline.model.Event;
import pipeline.model.WindowResult;
import pipeline.sink.UpsertSink;

import java.sql.Types;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka -> validate (DLQ) -> lateness (checkpointed watermark) -> de-duplicate -> 10 s event-time windows
 * -> idempotent Postgres upserts; finalized windows (after allowed lateness) -> EWMA detector -> alerts.
 * Semantics: docs/02-semantics.md. Every operator has a stable uid so checkpoints restore by name.
 */
public class StreamingJob {
    static final OutputTag<Envelope> DLQ = new OutputTag<>("dlq") {};
    static final OutputTag<Event> LATE = new OutputTag<>("late") {};
    static final OutputTag<Event> WINDOW_LATE = new OutputTag<>("window-operator-late") {};

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = parse(argv);
        Constants k = Constants.load(a.get("constants"));
        String jdbc = a.get("jdbc-url");
        long sinkFlushMs = Long.parseLong(a.getOrDefault("sink-flush-ms", "0"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(a.getOrDefault("parallelism", "2")));
        env.enableCheckpointing(Long.parseLong(a.getOrDefault("checkpoint-ms", "5000")), CheckpointingMode.EXACTLY_ONCE);
        // Flush network buffers every 20 ms (default 100 ms): bounds how far a watermark on one
        // channel can trail another, in event time, at high replay speed.
        env.setBufferTimeout(20);

        KafkaSource<Envelope> source = KafkaSource.<Envelope>builder()
                .setBootstrapServers(a.get("bootstrap"))
                .setTopics(a.get("topic"))
                .setGroupId(a.get("group"))
                // Fresh jobs start at the beginning; restored jobs resume from offsets in the checkpoint.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new EnvelopeDeserializer(k))
                .build();
        long bound = k.boundMs;
        WatermarkStrategy<Envelope> watermarks = WatermarkStrategy
                .<Envelope>forGenerator(ctx -> new PerEventWatermarks(bound))
                // Invalid records carry no event time; MIN_VALUE never advances the watermark.
                .withTimestampAssigner((e, ts) -> e.event != null ? e.event.tsMs : Long.MIN_VALUE);

        // Source, validate and lateness all at parallelism 1 with forward connections (one chained
        // task) on a single-partition topic: processing order = arrival order, so late-event
        // decisions are a function of the data. A parallel validate step once reordered records
        // round-robin before the lateness filter; eval.run_pipeline now checks the job graph.
        SingleOutputStreamOperator<Event> valid = env.fromSource(source, watermarks, "kafka-events").uid("source")
                .setParallelism(1)
                .process(new ProcessFunction<Envelope, Event>() {
                    @Override
                    public void processElement(Envelope e, Context ctx, Collector<Event> out) {
                        if (e.event == null) ctx.output(DLQ, e); else out.collect(e.event);
                    }
                }).setParallelism(1).uid("validate").name("validate");

        valid.getSideOutput(DLQ).sinkTo(new UpsertSink<Envelope>(jdbc,
                "INSERT INTO dlq (kafka_partition, kafka_offset, reason, raw) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (ps, e) -> { ps.setInt(1, e.partition); ps.setLong(2, e.offset); ps.setString(3, e.reason); ps.setString(4, e.raw); },
                200)).uid("sink-dlq").name("sink-dlq");

        // Lateness is decided here, with a checkpointed watermark (see LatenessFilter), before dedup.
        SingleOutputStreamOperator<Event> onTime = valid
                .process(new LatenessFilter(k, LATE)).setParallelism(1).uid("lateness").name("lateness");
        SingleOutputStreamOperator<WindowResult> windows = onTime
                .keyBy(e -> e.eventId).process(new Dedup(k.dedupHorizonMs)).uid("dedup").name("dedup")
                .keyBy(e -> e.service)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(k.windowMs)))
                .allowedLateness(Duration.ofMillis(k.latenessMs))
                // Tripwire: should stay empty, because this operator's watermark never passes the filter's.
                .sideOutputLateData(WINDOW_LATE)
                .process(new WindowMetrics(k.errorStatusMin)).uid("windows").name("windows");

        windows.sinkTo(new UpsertSink<WindowResult>(jdbc,
                "INSERT INTO windows (service, window_start_ms, request_count, error_count, latency_p95_ms, queue_depth_max) "
                        + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (service, window_start_ms) DO UPDATE SET "
                        + "request_count = EXCLUDED.request_count, error_count = EXCLUDED.error_count, "
                        + "latency_p95_ms = EXCLUDED.latency_p95_ms, queue_depth_max = EXCLUDED.queue_depth_max, "
                        + "last_written_at = clock_timestamp(), writes = windows.writes + 1",
                (ps, r) -> { ps.setString(1, r.service); ps.setLong(2, r.windowStartMs); ps.setInt(3, r.requestCount);
                             ps.setInt(4, r.errorCount); ps.setDouble(5, r.latencyP95Ms); ps.setInt(6, r.queueDepthMax); },
                200, sinkFlushMs)).uid("sink-windows").name("sink-windows");

        long windowMs = k.windowMs;
        onTime.getSideOutput(LATE).sinkTo(new UpsertSink<Event>(jdbc,
                "INSERT INTO late_events (event_id, service, window_start_ms) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                (ps, e) -> { ps.setString(1, e.eventId); ps.setString(2, e.service); ps.setLong(3, e.tsMs - Math.floorMod(e.tsMs, windowMs)); },
                100)).setParallelism(1).uid("sink-late").name("sink-late");
        windows.getSideOutput(WINDOW_LATE).sinkTo(new UpsertSink<Event>(jdbc,
                "INSERT INTO window_operator_late (event_id) VALUES (?) ON CONFLICT DO NOTHING",
                (ps, e) -> ps.setString(1, e.eventId), 1)).uid("sink-window-late").name("sink-window-late");

        windows.keyBy(r -> r.service).process(new EwmaDetector(k)).uid("detector").name("detector")
                .sinkTo(new UpsertSink<Alert>(jdbc,
                        "INSERT INTO alerts (alert_id, service, episode_start_ms, opened_window_ms, closed_window_ms, metrics, z_at_open) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (alert_id) DO UPDATE SET closed_window_ms = EXCLUDED.closed_window_ms",
                        (ps, al) -> { ps.setString(1, al.alertId); ps.setString(2, al.service); ps.setLong(3, al.episodeStartMs);
                                      ps.setLong(4, al.openedWindowMs);
                                      if (al.closedWindowMs == null) ps.setNull(5, Types.BIGINT); else ps.setLong(5, al.closedWindowMs);
                                      ps.setString(6, al.metrics); ps.setDouble(7, al.zAtOpen); },
                        50, sinkFlushMs)).uid("sink-alerts").name("sink-alerts");

        env.execute(a.getOrDefault("name", "stream-anomaly"));
    }

    static Map<String, String> parse(String[] argv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < argv.length; i += 2) m.put(argv[i].replaceFirst("^--", ""), argv[i + 1]);
        for (String req : new String[] {"bootstrap", "topic", "group", "jdbc-url", "constants"})
            if (!m.containsKey(req)) throw new IllegalArgumentException("missing --" + req);
        return m;
    }
}
