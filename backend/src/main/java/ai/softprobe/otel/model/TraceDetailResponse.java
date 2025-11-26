package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class TraceDetailResponse {
    private boolean success;
    private String message;
    private String traceId;
    private String spanId;
    private String parentSpanId;
    private String name;
    private String kind;
    private Instant startTime;
    private Instant endTime;
    private Long duration;
    private String status;
    private Map<String, Object> attributes;
    private String serviceName;
    private String spanType;

    // Request and Response logs
    private List<LogEntry> requestLogs;
    private List<LogEntry> responseLogs;

    @Data
    @Builder
    public static class LogEntry {
        private Instant timestamp;
        private String level;
        private String body;
        private Map<String, Object> attributes;
        private String traceId;
        private String spanId;
    }
}

