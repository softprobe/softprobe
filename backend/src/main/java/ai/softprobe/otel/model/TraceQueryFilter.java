package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

import java.time.LocalDateTime;
import java.time.Duration;
import java.util.Map;

@Data
@Builder
public class TraceQueryFilter {
    private String traceId;
    private String serviceName;
    private String operationName;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Duration minDuration;
    private Duration maxDuration;
    private String status;
    private String spanType;
    private Map<String, String> attributes;
    private int limit;
    private int offset;
}

