package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

@Data
@Builder
public class TraceDetailRequest {
    private String traceId;
    private String spanId;
}

