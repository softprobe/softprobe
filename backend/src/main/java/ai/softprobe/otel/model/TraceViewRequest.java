package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

@Data
@Builder
public class TraceViewRequest {
    private String traceId;
}

