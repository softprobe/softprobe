package ai.softprobe.otel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;

import java.time.Duration;

@ConfigurationProperties(prefix = "clickhouse")
@Data
public class ClickHouseProperties {
    private String url = "jdbc:clickhouse://localhost:8123/default";
    private String database = "otel";
    private String username = "default";
    private String password = "";
    private String logsTableId = "logs";
    private String metricsTableId = "metrics";
    private String spansTableId = "spans";
    private String envoyProxySpansTableId = "envoy_proxy_spans";
    
    // Query-specific properties
    private int defaultPageSize = 100;
    private int maxPageSize = 1000;
    private Duration queryTimeout = Duration.ofSeconds(30);
}

