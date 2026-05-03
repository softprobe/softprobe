package ai.softprobe.otel.repository;

import ai.softprobe.otel.model.RepositoryMetaInfo;
import ai.softprobe.otel.config.ClickHouseProperties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.common.v1.KeyValue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.zip.CRC32;

@Slf4j
@Repository
@Primary
public class OtelClickHouseRepository implements LogRepository, MetricRepository, SpanRepository {

    @Autowired
    private DataSource clickHouseDataSource;

    @Autowired
    private ClickHouseProperties properties;

    private final Gson gson = new GsonBuilder().create();

    @jakarta.annotation.PostConstruct
    public void createTablesIfNotExist() {
        try (Connection conn = clickHouseDataSource.getConnection()) {
            createDatabaseIfNotExists(conn);
            createTableIfNotExists(conn, properties.getLogsTableId(), createLogsTableDDL());
            createTableIfNotExists(conn, properties.getMetricsTableId(), createMetricsTableDDL());
            createTableIfNotExists(conn, properties.getSpansTableId(), createSpansTableDDL());
            createTableIfNotExists(conn, properties.getEnvoyProxySpansTableId(), createEnvoyProxySpansTableDDL());
        } catch (SQLException e) {
            log.error("Failed to create tables: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize ClickHouse tables", e);
        }
    }

    private void createDatabaseIfNotExists(Connection conn) throws SQLException {
        String sql = String.format("CREATE DATABASE IF NOT EXISTS %s", properties.getDatabase());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("Database {} created or already exists", properties.getDatabase());
        }
    }

    private void createTableIfNotExists(Connection conn, String tableName, String ddl) throws SQLException {
        String fullTableName = properties.getDatabase() + "." + tableName;
        // Use appropriate ORDER BY based on table type
        String orderBy;
        if ("logs".equals(tableName)) {
            orderBy = "ORDER BY (timestamp, trace_id, span_id)";
        } else if ("metrics".equals(tableName)) {
            orderBy = "ORDER BY (timestamp, metric_name)";
        } else if ("envoy_proxy_spans".equals(tableName)) {
            orderBy = "ORDER BY (start_time, session_id, trace_id, span_id)";
        } else {
            orderBy = "ORDER BY (start_time, trace_id, span_id)";
        }
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s %s ENGINE = MergeTree() %s", 
            fullTableName, ddl, orderBy);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("Table {} created or already exists", fullTableName);
        }
    }

    private String createLogsTableDDL() {
        return "(" +
            "timestamp DateTime," +
            "trace_id String," +
            "span_id String," +
            "resource String," +
            "severity String," +
            "body String," +
            "attributes String" +
            ")";
    }

    private String createMetricsTableDDL() {
        return "(" +
            "timestamp DateTime," +
            "start_time DateTime," +
            "resource String," +
            "metric_name String," +
            "metric_description String," +
            "metric_unit String," +
            "metric_type String," +
            "aggregation_temporality String," +
            "is_monotonic UInt8," +
            "metric_value Float64," +
            "metric_value_count Int64," +
            "metric_value_sum Float64," +
            "bucket_counts String," +
            "explicit_bounds String," +
            "exponential_histogram_scale Int64," +
            "exponential_histogram_zero_count Int64," +
            "exponential_histogram_positive_buckets_offset Int64," +
            "exponential_histogram_positive_buckets_counts String," +
            "exponential_histogram_negative_buckets_offset Int64," +
            "exponential_histogram_negative_buckets_counts String," +
            "quantile_values String," +
            "attributes String" +
            ")";
    }

    private String createSpansTableDDL() {
        return "(" +
            "trace_id String," +
            "span_id String," +
            "parent_span_id String," +
            "trace_state String," +
            "kind String," +
            "name String," +
            "start_time DateTime," +
            "end_time DateTime," +
            "resource String," +
            "attributes String," +
            "events String," +
            "links String," +
            "status String," +
            "request_body_hash String," +
            "span_type String" +
            ")";
    }

    private String createEnvoyProxySpansTableDDL() {
        return "(" +
            "session_id String," +
            "trace_id String," +
            "span_id String," +
            "parent_span_id String," +
            "name String," +
            "start_time DateTime," +
            "end_time DateTime," +
            "http_request_body String," +
            "http_response_body String," +
            "http_method String," +
            "http_url String," +
            "http_status_code Int64," +
            "service_name String," +
            "attributes String," +
            "resource String," +
            "created_at DateTime" +
            ")";
    }

    @Override
    public void storeLogs(List<LogRecord> logRecords, Resource resource) {
        if (logRecords == null || logRecords.isEmpty()) {
            return;
        }

        String sql = String.format(
            "INSERT INTO %s.%s (timestamp, trace_id, span_id, resource, severity, body, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)",
            properties.getDatabase(), properties.getLogsTableId()
        );

        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (LogRecord log : logRecords) {
                Timestamp timestamp = new Timestamp(log.getTimeUnixNano() / 1_000_000);
                String traceId = log.getTraceId().isEmpty() ? null : bytesToHex(log.getTraceId().toByteArray());
                String spanId = log.getSpanId().isEmpty() ? null : bytesToHex(log.getSpanId().toByteArray());
                String resourceJson = gson.toJson(transformKeyValueList(resource.getAttributesList()));
                String severity = log.getSeverityText();
                String body = log.getBody().getStringValue();
                String attributesJson = gson.toJson(transformKeyValueList(log.getAttributesList()));

                pstmt.setTimestamp(1, timestamp);
                pstmt.setString(2, traceId);
                pstmt.setString(3, spanId);
                pstmt.setString(4, resourceJson);
                pstmt.setString(5, severity);
                pstmt.setString(6, body);
                pstmt.setString(7, attributesJson);
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            log.info("Stored {} logs", logRecords.size());
        } catch (SQLException e) {
            log.error("Failed to store logs: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to store logs", e);
        }
    }

    @Override
    public void storeMetrics(List<Metric> metrics, Resource resource) {
        // Simplified implementation - can be enhanced later
        log.info("Metrics storage not fully implemented yet");
    }

    @Override
    public void storeSpans(List<Span> spans, Resource resource, RepositoryMetaInfo metaInfo) {
        String database = (metaInfo != null && metaInfo.getDatabase() != null) 
            ? metaInfo.getDatabase() 
            : properties.getDatabase();
        storeSpans(spans, resource, database);
    }

    public void storeSpans(List<Span> spans, Resource resource, String database) {
        if (spans == null || spans.isEmpty()) {
            return;
        }

        List<Map<String, Object>> regularSpanRows = new ArrayList<>();
        List<Map<String, Object>> envoyProxySpanRows = new ArrayList<>();

        for (Span span : spans) {
            if (isEnvoyProxySpan(span, resource)) {
                Map<String, Object> row = transformEnvoyProxySpan(span, resource);
                if (row != null) {
                    envoyProxySpanRows.add(row);
                }
            }
            regularSpanRows.add(transformSpan(span, resource));
        }

        if (!regularSpanRows.isEmpty()) {
            insertSpans(database, properties.getSpansTableId(), regularSpanRows);
        }

        if (!envoyProxySpanRows.isEmpty()) {
            insertEnvoyProxySpans(database, properties.getEnvoyProxySpansTableId(), envoyProxySpanRows);
        }
    }

    private void insertSpans(String database, String tableName, List<Map<String, Object>> rows) {
        String sql = String.format(
            "INSERT INTO %s.%s (trace_id, span_id, parent_span_id, trace_state, kind, name, start_time, end_time, resource, attributes, events, links, status, request_body_hash, span_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            database, tableName
        );

        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (Map<String, Object> row : rows) {
                pstmt.setString(1, (String) row.get("trace_id"));
                pstmt.setString(2, (String) row.get("span_id"));
                pstmt.setString(3, (String) row.get("parent_span_id"));
                pstmt.setString(4, (String) row.get("trace_state"));
                pstmt.setString(5, (String) row.get("kind"));
                pstmt.setString(6, (String) row.get("name"));
                
                pstmt.setTimestamp(7, (Timestamp) row.get("start_time"));
                pstmt.setTimestamp(8, (Timestamp) row.get("end_time"));
                pstmt.setString(9, (String) row.get("resource"));
                pstmt.setString(10, (String) row.get("attributes"));
                pstmt.setString(11, (String) row.get("events"));
                pstmt.setString(12, (String) row.get("links"));
                pstmt.setString(13, (String) row.get("status"));
                pstmt.setString(14, (String) row.get("request_body_hash"));
                pstmt.setString(15, (String) row.get("span_type"));
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            log.info("Stored {} spans", rows.size());
        } catch (SQLException e) {
            log.error("Failed to store spans: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to store spans", e);
        }
    }

    private void insertEnvoyProxySpans(String database, String tableName, List<Map<String, Object>> rows) {
        String sql = String.format(
            "INSERT INTO %s.%s (session_id, trace_id, span_id, parent_span_id, name, start_time, end_time, http_request_body, http_response_body, http_method, http_url, http_status_code, service_name, attributes, resource, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            database, tableName
        );

        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (Map<String, Object> row : rows) {
                pstmt.setString(1, (String) row.get("session_id"));
                pstmt.setString(2, (String) row.get("trace_id"));
                pstmt.setString(3, (String) row.get("span_id"));
                pstmt.setString(4, (String) row.get("parent_span_id"));
                pstmt.setString(5, (String) row.get("name"));
                
                pstmt.setTimestamp(6, (Timestamp) row.get("start_time"));
                pstmt.setTimestamp(7, (Timestamp) row.get("end_time"));
                pstmt.setString(8, (String) row.get("http_request_body"));
                pstmt.setString(9, (String) row.get("http_response_body"));
                pstmt.setString(10, (String) row.get("http_method"));
                pstmt.setString(11, (String) row.get("http_url"));
                pstmt.setObject(12, row.get("http_status_code"));
                pstmt.setString(13, (String) row.get("service_name"));
                pstmt.setString(14, (String) row.get("attributes"));
                pstmt.setString(15, (String) row.get("resource"));
                pstmt.setTimestamp(16, (Timestamp) row.get("created_at"));
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            log.info("Stored {} envoy proxy spans", rows.size());
        } catch (SQLException e) {
            log.error("Failed to store envoy proxy spans: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to store envoy proxy spans", e);
        }
    }

    @Override
    public Span findMatchingSpan(Span incomingSpan) {
        // Simplified - can be enhanced with cache lookup
        return null;
    }

    private Map<String, Object> transformSpan(Span span, Resource resource) {
        Map<String, Object> row = new HashMap<>();
        row.put("trace_id", bytesToHex(span.getTraceId().toByteArray()));
        row.put("span_id", bytesToHex(span.getSpanId().toByteArray()));
        row.put("parent_span_id", span.getParentSpanId().isEmpty() ? null : bytesToHex(span.getParentSpanId().toByteArray()));
        row.put("trace_state", span.getTraceState());
        row.put("kind", span.getKind().toString());
        row.put("name", span.getName());
        
        row.put("start_time", Timestamp.from(Instant.ofEpochSecond(0, span.getStartTimeUnixNano())));
        row.put("end_time", Timestamp.from(Instant.ofEpochSecond(0, span.getEndTimeUnixNano())));
        
        row.put("resource", gson.toJson(transformKeyValueList(resource.getAttributesList())));
        row.put("attributes", gson.toJson(transformKeyValueList(span.getAttributesList())));
        row.put("events", gson.toJson(transformEvents(span.getEventsList())));
        row.put("links", gson.toJson(transformLinks(span.getLinksList())));
        row.put("status", span.getStatus().getCode().toString());
        
        String requestBodyHash = extractAndHashRequestBody(span);
        row.put("request_body_hash", requestBodyHash);
        
        String spanType = extractSpanType(span);
        row.put("span_type", spanType);
        
        return row;
    }

    private Map<String, Object> transformEnvoyProxySpan(Span span, Resource resource) {
        Map<String, Object> attributes = transformKeyValueList(span.getAttributesList());
        String sessionId = (String) attributes.get("sp_session_id");
        if (sessionId == null || sessionId.trim().isEmpty()) {
            return null;
        }

        Map<String, Object> row = new HashMap<>();
        row.put("session_id", sessionId);
        row.put("trace_id", bytesToHex(span.getTraceId().toByteArray()));
        row.put("span_id", bytesToHex(span.getSpanId().toByteArray()));
        row.put("parent_span_id", span.getParentSpanId().isEmpty() ? null : bytesToHex(span.getParentSpanId().toByteArray()));
        row.put("name", span.getName());
        
        row.put("start_time", Timestamp.from(Instant.ofEpochSecond(0, span.getStartTimeUnixNano())));
        row.put("end_time", Timestamp.from(Instant.ofEpochSecond(0, span.getEndTimeUnixNano())));
        
        row.put("http_request_body", (String) attributes.get("http_request_body"));
        row.put("http_response_body", (String) attributes.get("http_response_body"));
        row.put("http_method", (String) attributes.get("http_request_header_:method"));
        row.put("http_url", (String) attributes.get("http_request_header_:path"));
        
        Object statusCode = attributes.get("http_response_status_code");
        if (statusCode == null) {
            statusCode = attributes.get("http.response.status_code");
        }
        row.put("http_status_code", statusCode);
        
        Map<String, Object> resourceMap = transformKeyValueList(resource.getAttributesList());
        row.put("service_name", resourceMap.get("service_name") != null ? resourceMap.get("service_name") : resourceMap.get("service.name"));
        row.put("attributes", gson.toJson(attributes));
        row.put("resource", gson.toJson(resourceMap));
        row.put("created_at", Timestamp.from(Instant.now()));
        
        return row;
    }

    private boolean isEnvoyProxySpan(Span span, Resource resource) {
        for (KeyValue attribute : resource.getAttributesList()) {
            if ("sp.resource.type".equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                if ("sp-envoy-proxy".equals(attribute.getValue().getStringValue())) {
                    return true;
                }
            }
        }
        
        for (KeyValue attribute : span.getAttributesList()) {
            if ("span.type".equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                if ("sp-envoy-proxy".equals(attribute.getValue().getStringValue())) {
                    return true;
                }
            }
        }
        
        return false;
    }

    private String extractSpanType(Span span) {
        for (KeyValue attribute : span.getAttributesList()) {
            if ("span.type".equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                return attribute.getValue().getStringValue();
            }
        }
        return null;
    }

    private String extractAndHashRequestBody(Span span) {
        for (KeyValue attribute : span.getAttributesList()) {
            if ("http.request.body".equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                String requestBody = attribute.getValue().getStringValue();
                if (requestBody != null && !requestBody.isEmpty()) {
                    CRC32 crc32 = new CRC32();
                    crc32.update(requestBody.getBytes(StandardCharsets.UTF_8));
                    return String.valueOf(crc32.getValue());
                }
            }
        }
        return null;
    }

    private Map<String, Object> transformKeyValueList(List<KeyValue> keyValueList) {
        Map<String, Object> result = new HashMap<>();
        for (KeyValue kv : keyValueList) {
            String key = kv.getKey().replace('.', '_');
            result.put(key, transformAnyValue(kv.getValue()));
        }
        return result;
    }

    private Object transformAnyValue(io.opentelemetry.proto.common.v1.AnyValue anyValue) {
        if (anyValue.hasStringValue()) {
            return anyValue.getStringValue();
        } else if (anyValue.hasIntValue()) {
            return anyValue.getIntValue();
        } else if (anyValue.hasDoubleValue()) {
            return anyValue.getDoubleValue();
        } else if (anyValue.hasBoolValue()) {
            return anyValue.getBoolValue();
        } else if (anyValue.hasBytesValue()) {
            return bytesToHex(anyValue.getBytesValue().toByteArray());
        } else if (anyValue.hasArrayValue()) {
            List<Object> array = new ArrayList<>();
            anyValue.getArrayValue().getValuesList().forEach(value -> array.add(transformAnyValue(value)));
            return array;
        } else if (anyValue.hasKvlistValue()) {
            return transformKeyValueList(anyValue.getKvlistValue().getValuesList());
        }
        return null;
    }

    private List<Map<String, Object>> transformEvents(List<Span.Event> eventsList) {
        List<Map<String, Object>> events = new ArrayList<>();
        eventsList.forEach(event -> {
            Map<String, Object> eventData = new HashMap<>();
            Timestamp timestamp = new Timestamp(event.getTimeUnixNano() / 1_000_000);
            eventData.put("time", timestamp.toString());
            eventData.put("name", event.getName());
            eventData.put("attributes", gson.toJson(transformKeyValueList(event.getAttributesList())));
            events.add(eventData);
        });
        return events;
    }

    private List<Map<String, Object>> transformLinks(List<Span.Link> linksList) {
        List<Map<String, Object>> links = new ArrayList<>();
        linksList.forEach(link -> {
            Map<String, Object> linkData = new HashMap<>();
            linkData.put("trace_id", bytesToHex(link.getTraceId().toByteArray()));
            linkData.put("span_id", bytesToHex(link.getSpanId().toByteArray()));
            linkData.put("trace_state", link.getTraceState());
            linkData.put("attributes", gson.toJson(transformKeyValueList(link.getAttributesList())));
            links.add(linkData);
        });
        return links;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

