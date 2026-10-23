package pipeline.model;

public class WindowResult {
    public String service;
    public long windowStartMs;
    public int requestCount;
    public int errorCount;
    public double latencyP95Ms;
    public int queueDepthMax;
    public boolean isUpdate;   // false on the window's first firing, true for late-event updates

    public WindowResult() {}
}
