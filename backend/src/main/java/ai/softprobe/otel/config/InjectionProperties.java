package ai.softprobe.otel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties(prefix = "injection")
@Data
public class InjectionProperties {
    private long cacheTtlSeconds = 86400; // 24 hours default TTL for cache entries
}

