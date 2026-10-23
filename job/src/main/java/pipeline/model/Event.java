package pipeline.model;

/** A validated event (docs/semantics.md). Public fields + no-arg constructor: a Flink POJO. */
public class Event {
    public String eventId;
    public String service;
    public long tsMs;
    public double latencyMs;
    public int status;
    public int queueDepth;

    public Event() {}
}
