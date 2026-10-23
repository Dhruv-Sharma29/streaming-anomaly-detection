package pipeline.model;

public class Alert {
    public String alertId;
    public String service;
    public long episodeStartMs;
    public long openedWindowMs;
    public Long closedWindowMs;  // null while open
    public String metrics;
    public double zAtOpen;

    public Alert() {}

    public Alert copy() {
        Alert a = new Alert();
        a.alertId = alertId; a.service = service; a.episodeStartMs = episodeStartMs;
        a.openedWindowMs = openedWindowMs; a.closedWindowMs = closedWindowMs;
        a.metrics = metrics; a.zAtOpen = zAtOpen;
        return a;
    }
}
