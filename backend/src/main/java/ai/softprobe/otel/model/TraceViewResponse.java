package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

import java.util.List;

@Data
@Builder
public class TraceViewResponse {
    private boolean success;
    private String message;
    private String traceId;
    private List<TraceSpanNode> spans;
    private long totalSpans;
    private long totalDuration;
}

