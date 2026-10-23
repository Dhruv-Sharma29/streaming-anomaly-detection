package pipeline;

import pipeline.model.Alert;
import pipeline.model.WindowResult;

import java.util.ArrayList;
import java.util.List;

/**
 * EWMA z-score detector, a line-by-line port of detector/ewma.py. Same constants, same
 * floating-point operations in the same order, so alerts must match the Python reference exactly
 * (checked by tests/test_detector_parity.py through DetectorCli).
 */
public final class EwmaModel {
    public static final String[] METRICS = {"latency_p95_ms", "error_rate", "queue_depth_max"};

    /** Per-service detector state; a Flink POJO so it can live in keyed, checkpointed state. */
    public static class State {
        public int n;
        public double[] mean = new double[3];
        public double[] var = new double[3];
        public int anomalousStreak;
        public int normalStreak;
        public long episodeStartMs;
        public Alert open;

        public State() {}
    }

    private EwmaModel() {}

    static double[] features(WindowResult r) {
        return new double[] {r.latencyP95Ms, (double) r.errorCount / (double) r.requestCount, (double) r.queueDepthMax};
    }

    static void update(Constants k, State st, double[] x) {
        for (int m = 0; m < 3; m++) {
            if (st.n == 0) {
                st.mean[m] = x[m];
                st.var[m] = 0.0;
            } else {
                double d = x[m] - st.mean[m];
                st.mean[m] = st.mean[m] + k.alpha * d;
                st.var[m] = (1.0 - k.alpha) * (st.var[m] + k.alpha * d * d);
            }
        }
        st.n += 1;
    }

    static double z(Constants k, State st, int m, double x) {
        double floor = Math.max(k.floorAbs[m], k.floorRel[m] * st.mean[m]);
        return (x - st.mean[m]) / Math.max(Math.sqrt(st.var[m]), floor);
    }

    /** Process one finalized window; returns alert records to upsert (open and/or close). */
    public static List<Alert> step(Constants k, State st, String service, WindowResult row) {
        List<Alert> out = new ArrayList<>();
        double[] x = features(row);
        long ws = row.windowStartMs;
        if (st.n < k.warmupWindows) {
            update(k, st, x);
            return out;
        }
        double[] z = new double[3];
        List<String> hot = new ArrayList<>();
        double zMax = Double.NEGATIVE_INFINITY;
        for (int m = 0; m < 3; m++) {
            z[m] = z(k, st, m, x[m]);
            if (z[m] > k.zThreshold) hot.add(METRICS[m]);
            zMax = Math.max(zMax, z[m]);
        }
        hot.sort(null);
        if (!hot.isEmpty()) {
            st.anomalousStreak += 1;
            st.normalStreak = 0;
            if (st.anomalousStreak == 1) st.episodeStartMs = ws;
            if (st.open == null && st.anomalousStreak >= k.openAfter) {
                Alert a = new Alert();
                a.alertId = service + ":" + st.episodeStartMs;
                a.service = service;
                a.episodeStartMs = st.episodeStartMs;
                a.openedWindowMs = ws;
                a.closedWindowMs = null;
                a.metrics = String.join(",", hot);
                a.zAtOpen = zMax;
                st.open = a;
                out.add(a.copy());
            }
        } else {
            st.normalStreak += 1;
            st.anomalousStreak = 0;
            update(k, st, x);
            if (st.open != null && st.normalStreak >= k.closeAfter) {
                st.open.closedWindowMs = ws;
                out.add(st.open.copy());
                st.open = null;
            }
        }
        return out;
    }
}
