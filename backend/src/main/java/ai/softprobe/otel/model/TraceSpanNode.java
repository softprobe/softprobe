package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class TraceSpanNode {
    private String spanId;
    private String parentSpanId;
    private String traceId;
    private String name;
    private String kind;
    private Instant startTime;
    private Instant endTime;
    private Long duration;
    private String status;
    private Map<String, Object> attributes;
    private String serviceName;
    private String spanType;

    private List<TraceSpanNode> children;
}

