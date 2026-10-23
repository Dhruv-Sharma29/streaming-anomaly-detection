package pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import pipeline.model.Envelope;
import pipeline.model.Event;

import java.nio.charset.StandardCharsets;

/**
 * Turns every Kafka record into an Envelope and never throws, so a bad record can't fail the job.
 * Validation mirrors semantics.parse() in Python check for check and in the same order, so both
 * assign the same dead-letter reason to the same input.
 */
public class EnvelopeDeserializer implements KafkaRecordDeserializationSchema<Envelope> {
    private final Constants k;
    private transient ObjectMapper mapper;

    public EnvelopeDeserializer(Constants k) { this.k = k; }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Envelope> out) {
        if (mapper == null) mapper = new ObjectMapper();
        Envelope env = new Envelope();
        env.partition = record.partition();
        env.offset = record.offset();
        String raw = record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
        String reason = validate(raw, env);
        if (reason != null) {
            env.reason = reason;
            env.raw = raw;
            env.event = null;
        }
        out.collect(env);
    }

    /** Returns null and fills env.event when valid; otherwise the dead-letter reason. */
    String validate(String raw, Envelope env) {
        JsonNode n;
        try {
            n = mapper.readTree(raw);
        } catch (Exception e) {
            return "malformed_json";
        }
        if (n == null || n.isMissingNode()) return "malformed_json";
        if (!n.isObject()) return "not_an_object";
        JsonNode v = n.get("schema_version");
        if (v == null || !v.isIntegralNumber() || !k.acceptedVersions.contains(v.asInt())) return "unsupported_schema_version";
        if (!isText(n, "event_id")) return "bad_field:event_id";
        if (!isText(n, "service")) return "bad_field:service";
        if (!isInt(n, "ts_ms")) return "bad_field:ts_ms";
        if (n.get("latency_ms") == null || !n.get("latency_ms").isNumber()) return "bad_field:latency_ms";
        if (!isInt(n, "status")) return "bad_field:status";
        if (!isInt(n, "queue_depth")) return "bad_field:queue_depth";
        Event e = new Event();
        e.eventId = n.get("event_id").asText();
        e.service = n.get("service").asText();
        e.tsMs = n.get("ts_ms").asLong();
        e.latencyMs = n.get("latency_ms").asDouble();
        e.status = n.get("status").asInt();
        e.queueDepth = n.get("queue_depth").asInt();
        if (e.service.isEmpty() || e.latencyMs < 0 || e.queueDepth < 0 || !Double.isFinite(e.latencyMs)) return "invalid_value";
        env.event = e;
        return null;
    }

    private static boolean isText(JsonNode n, String f) { return n.get(f) != null && n.get(f).isTextual(); }
    private static boolean isInt(JsonNode n, String f) { return n.get(f) != null && n.get(f).isIntegralNumber(); }

    @Override
    public TypeInformation<Envelope> getProducedType() { return TypeInformation.of(Envelope.class); }
}
