package pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/** Stream semantics shared with the Python reference: both read semantics/constants.json. */
public final class Constants implements Serializable {
    public long windowMs, boundMs, latenessMs, dedupHorizonMs;
    public int errorStatusMin;
    public Set<Integer> acceptedVersions = new HashSet<>();
    public String flushService;
    // detector
    public double alpha, zThreshold;
    public int warmupWindows, openAfter, closeAfter;
    public double[] floorAbs = new double[3], floorRel = new double[3];

    public static Constants load(String path) throws IOException {
        JsonNode c = new ObjectMapper().readTree(Files.readString(Path.of(path)));
        Constants k = new Constants();
        k.windowMs = c.get("window_ms").asLong();
        k.boundMs = c.get("watermark_bound_ms").asLong();
        k.latenessMs = c.get("allowed_lateness_ms").asLong();
        k.dedupHorizonMs = c.get("dedup_horizon_ms").asLong();
        k.errorStatusMin = c.get("error_status_min").asInt();
        c.get("accepted_schema_versions").forEach(v -> k.acceptedVersions.add(v.asInt()));
        k.flushService = c.get("flush_service").asText();
        JsonNode d = c.get("detector");
        k.alpha = d.get("alpha").asDouble();
        k.zThreshold = d.get("z_threshold").asDouble();
        k.warmupWindows = d.get("warmup_windows").asInt();
        k.openAfter = d.get("open_after").asInt();
        k.closeAfter = d.get("close_after").asInt();
        for (int i = 0; i < 3; i++) {
            JsonNode f = d.get("std_floor").get(EwmaModel.METRICS[i]);
            k.floorAbs[i] = f.get("abs").asDouble();
            k.floorRel[i] = f.get("rel").asDouble();
        }
        return k;
    }
}
