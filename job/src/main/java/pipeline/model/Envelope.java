package pipeline.model;

/** Every Kafka record becomes an Envelope: a parsed event, or the raw value plus a dead-letter reason. */
public class Envelope {
    public Event event;      // null when invalid
    public String reason;    // dead-letter reason when invalid
    public String raw;       // kept only for invalid records
    public int partition;
    public long offset;

    public Envelope() {}
}
