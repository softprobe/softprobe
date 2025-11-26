package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;

import java.time.Instant;
import java.util.List;

@Data
@Builder
public class SessionListResponse {
    private boolean success;
    private String message;
    private List<SessionInfo> sessions;
    private long totalCount;
    private Instant createdAt;
    private int pageSize;
    private String nextCursor;
    private Boolean hasMore;
}

