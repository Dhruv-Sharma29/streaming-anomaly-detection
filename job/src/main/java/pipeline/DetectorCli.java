package pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import pipeline.model.Alert;
import pipeline.model.WindowResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Runs EwmaModel over a JSON array of first-firing windows (the reference's format) and prints the
 * final alerts as JSON. Lets tests compare the Java detector with the Python one without a cluster.
 *   java -cp stream-job.jar pipeline.DetectorCli constants.json first_firings.json
 */
public class DetectorCli {
    public static void main(String[] argv) throws Exception {
        Constants k = Constants.load(argv[0]);
        ObjectMapper om = new ObjectMapper();
        List<WindowResult> rows = new ArrayList<>();
        for (JsonNode n : om.readTree(Files.readString(Path.of(argv[1])))) {
            WindowResult r = new WindowResult();
            r.service = n.get("service").asText();
            r.windowStartMs = n.get("window_start_ms").asLong();
            r.requestCount = n.get("request_count").asInt();
            r.errorCount = n.get("error_count").asInt();
            r.latencyP95Ms = n.get("latency_p95_ms").asDouble();
            r.queueDepthMax = n.get("queue_depth_max").asInt();
            rows.add(r);
        }
        rows.sort(Comparator.<WindowResult, String>comparing(r -> r.service).thenComparingLong(r -> r.windowStartMs));
        Map<String, EwmaModel.State> states = new HashMap<>();
        Map<String, Alert> alerts = new TreeMap<>();
        for (WindowResult r : rows) {
            EwmaModel.State st = states.computeIfAbsent(r.service, s -> new EwmaModel.State());
            for (Alert a : EwmaModel.step(k, st, r.service, r)) alerts.put(a.alertId, a);
        }
        ArrayNode out = om.createArrayNode();
        for (Alert a : alerts.values()) {
            ObjectNode o = out.addObject();
            o.put("alert_id", a.alertId);
            o.put("service", a.service);
            o.put("episode_start_ms", a.episodeStartMs);
            o.put("opened_window_ms", a.openedWindowMs);
            if (a.closedWindowMs == null) o.putNull("closed_window_ms"); else o.put("closed_window_ms", a.closedWindowMs);
            o.put("metrics", a.metrics);
            o.put("z_at_open", a.zAtOpen);
        }
        System.out.println(om.writeValueAsString(out));
    }
}
