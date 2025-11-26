package ai.softprobe.otel.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Model for session information including creation time
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SessionInfo {
    
    /**
     * Session ID
     */
    private String sessionId;
    
    /**
     * Session creation time (earliest span creation time for this session)
     */
    private Instant createdAt;
    
    /**
     * Service name associated with the session
     */
    private String serviceName;
    
    /**
     * HTTP status code for the session
     */
    private String httpStatusCode;

    private List<String> spanIds;
}

