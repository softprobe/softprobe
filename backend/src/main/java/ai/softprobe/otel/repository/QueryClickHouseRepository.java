package ai.softprobe.otel.repository;

import ai.softprobe.otel.config.ClickHouseProperties;
import ai.softprobe.otel.model.TraceQueryFilter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.common.v1.KeyValue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class QueryClickHouseRepository implements QueryRepository {
    
    @Autowired
    private DataSource clickHouseDataSource;
    
    @Autowired
    private ClickHouseProperties properties;
    
    private final Gson gson = new GsonBuilder().create();
    
    @Override
    public ExportTraceServiceRequest queryTraces(TraceQueryFilter filter) {
        String sql = buildQuery(filter);
        log.info("Executing query: {}", sql);
        
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            setParameters(pstmt, filter);
            
            List<ResourceSpans> resourceSpans = new ArrayList<>();
            try (ResultSet rs = pstmt.executeQuery()) {
                Map<String, Map<String, List<Span>>> traceResourceSpans = new HashMap<>();
                
                while (rs.next()) {
                    try {
                        Span span = buildSpanFromRow(rs);
                        String traceId = rs.getString("trace_id");
                        String resourceJson = rs.getString("resource");
                        
                        traceResourceSpans
                            .computeIfAbsent(traceId, k -> new HashMap<>())
                            .computeIfAbsent(resourceJson, k -> new ArrayList<>())
                            .add(span);
                    } catch (Exception e) {
                        log.warn("Failed to process row: {}", e.getMessage());
                    }
                }
                
                resourceSpans = traceResourceSpans.entrySet().stream()
                    .flatMap(traceEntry -> 
                        traceEntry.getValue().entrySet().stream().map(resourceEntry -> {
                            try {
                                Resource resource = buildResourceFromJson(resourceEntry.getKey());
                                
                                ScopeSpans librarySpans = ScopeSpans.newBuilder()
                                    .addAllSpans(resourceEntry.getValue())
                                    .build();
                                
                                return ResourceSpans.newBuilder()
                                    .setResource(resource)
                                    .addScopeSpans(librarySpans)
                                    .build();
                            } catch (Exception e) {
                                log.warn("Failed to build ResourceSpans: {}", e.getMessage());
                                return null;
                            }
                        })
                    )
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            }
            
            return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(resourceSpans)
                .build();
        } catch (SQLException e) {
            log.error("Failed to query traces: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to query traces: " + e.getMessage(), e);
        }
    }
    
    private String buildQuery(TraceQueryFilter filter) {
        StringBuilder sql = new StringBuilder();
        
        sql.append("SELECT ")
           .append("trace_id, span_id, parent_span_id, name, kind, ")
           .append("start_time, end_time, resource, attributes, events, links, status, span_type ")
           .append("FROM ").append(properties.getDatabase()).append(".").append(properties.getSpansTableId()).append(" ")
           .append("WHERE 1=1 ");
        
        if (filter.getTraceId() != null) {
            sql.append("AND trace_id = ? ");
        }
        
        if (filter.getServiceName() != null) {
            sql.append("AND JSONExtractString(resource, 'service_name') = ? ");
        }
        
        if (filter.getOperationName() != null) {
            sql.append("AND name = ? ");
        }
        
        if (filter.getStartTime() != null) {
            sql.append("AND start_time >= ? ");
        }
        
        if (filter.getEndTime() != null) {
            sql.append("AND end_time <= ? ");
        }
        
        if (filter.getMinDuration() != null) {
            sql.append("AND dateDiff('microsecond', start_time, end_time) >= ? ");
        }
        
        if (filter.getMaxDuration() != null) {
            sql.append("AND dateDiff('microsecond', start_time, end_time) <= ? ");
        }
        
        if (filter.getStatus() != null) {
            sql.append("AND status = ? ");
        }
        
        if (filter.getSpanType() != null) {
            if ("empty".equals(filter.getSpanType())) {
                sql.append("AND (span_type IS NULL OR span_type = '') ");
            } else {
                sql.append("AND span_type = ? ");
            }
        }
        
        sql.append("ORDER BY start_time DESC ")
           .append("LIMIT ? OFFSET ?");
        
        log.info("Generated query: {}", sql.toString());
        return sql.toString();
    }
    
    private void setParameters(PreparedStatement pstmt, TraceQueryFilter filter) throws SQLException {
        int paramIndex = 1;
        
        if (filter.getTraceId() != null) {
            pstmt.setString(paramIndex++, filter.getTraceId());
        }
        
        if (filter.getServiceName() != null) {
            pstmt.setString(paramIndex++, filter.getServiceName());
        }
        
        if (filter.getOperationName() != null) {
            pstmt.setString(paramIndex++, filter.getOperationName());
        }
        
        if (filter.getStartTime() != null) {
            pstmt.setTimestamp(paramIndex++, Timestamp.valueOf(filter.getStartTime()));
        }
        
        if (filter.getEndTime() != null) {
            pstmt.setTimestamp(paramIndex++, Timestamp.valueOf(filter.getEndTime()));
        }
        
        if (filter.getMinDuration() != null) {
            pstmt.setLong(paramIndex++, filter.getMinDuration().toNanos() / 1000);
        }
        
        if (filter.getMaxDuration() != null) {
            pstmt.setLong(paramIndex++, filter.getMaxDuration().toNanos() / 1000);
        }
        
        if (filter.getStatus() != null) {
            pstmt.setString(paramIndex++, filter.getStatus());
        }
        
        if (filter.getSpanType() != null && !"empty".equals(filter.getSpanType())) {
            pstmt.setString(paramIndex++, filter.getSpanType());
        }
        
        pstmt.setInt(paramIndex++, filter.getLimit());
        pstmt.setInt(paramIndex++, filter.getOffset());
    }
    
    private Span buildSpanFromRow(ResultSet rs) throws SQLException {
        Span.Builder spanBuilder = Span.newBuilder();
        
        spanBuilder.setTraceId(com.google.protobuf.ByteString.copyFrom(hexToBytes(rs.getString("trace_id"))));
        spanBuilder.setSpanId(com.google.protobuf.ByteString.copyFrom(hexToBytes(rs.getString("span_id"))));
        
        String parentSpanId = rs.getString("parent_span_id");
        if (parentSpanId != null && !parentSpanId.isEmpty()) {
            spanBuilder.setParentSpanId(com.google.protobuf.ByteString.copyFrom(hexToBytes(parentSpanId)));
        }
        
        spanBuilder.setName(rs.getString("name"));
        spanBuilder.setKind(Span.SpanKind.valueOf(rs.getString("kind")));
        
        Timestamp startTime = rs.getTimestamp("start_time");
        Timestamp endTime = rs.getTimestamp("end_time");
        spanBuilder.setStartTimeUnixNano(startTime.getTime() * 1_000_000);
        spanBuilder.setEndTimeUnixNano(endTime.getTime() * 1_000_000);
        
        // Parse attributes
        String attributesJson = rs.getString("attributes");
        if (attributesJson != null) {
            List<KeyValue> attributes = parseAttributesFromJson(attributesJson);
            spanBuilder.addAllAttributes(attributes);
        }
        
        // Parse events
        String eventsJson = rs.getString("events");
        if (eventsJson != null) {
            List<Span.Event> events = parseEventsFromJson(eventsJson);
            spanBuilder.addAllEvents(events);
        }
        
        // Parse links
        String linksJson = rs.getString("links");
        if (linksJson != null) {
            List<Span.Link> links = parseLinksFromJson(linksJson);
            spanBuilder.addAllLinks(links);
        }
        
        // Set status
        String status = rs.getString("status");
        if (status != null) {
            spanBuilder.setStatus(io.opentelemetry.proto.trace.v1.Status.newBuilder()
                .setCode(io.opentelemetry.proto.trace.v1.Status.StatusCode.valueOf(status))
                .build());
        }
        
        return spanBuilder.build();
    }
    
    private Resource buildResourceFromJson(String resourceJson) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resourceMap = gson.fromJson(resourceJson, Map.class);
            List<KeyValue> attributes = convertMapToKeyValueList(resourceMap);
            
            return Resource.newBuilder()
                .addAllAttributes(attributes)
                .build();
        } catch (Exception e) {
            log.warn("Failed to parse resource JSON: {}", resourceJson, e);
            return Resource.getDefaultInstance();
        }
    }
    
    private List<KeyValue> parseAttributesFromJson(String attributesJson) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> attributesMap = gson.fromJson(attributesJson, Map.class);
            return convertMapToKeyValueList(attributesMap);
        } catch (Exception e) {
            log.warn("Failed to parse attributes JSON: {}", attributesJson, e);
            return new ArrayList<>();
        }
    }
    
    private List<Span.Event> parseEventsFromJson(String eventsJson) {
        // Simplified implementation - would need full event parsing
        return new ArrayList<>();
    }
    
    private List<Span.Link> parseLinksFromJson(String linksJson) {
        // Simplified implementation - would need full link parsing
        return new ArrayList<>();
    }
    
    private List<KeyValue> convertMapToKeyValueList(Map<String, Object> attributesMap) {
        return attributesMap.entrySet().stream()
            .map(entry -> KeyValue.newBuilder()
                .setKey(entry.getKey().replace('_', '.'))
                .setValue(convertObjectToAnyValue(entry.getValue()))
                .build())
            .collect(Collectors.toList());
    }
    
    private io.opentelemetry.proto.common.v1.AnyValue convertObjectToAnyValue(Object val) {
        if (val instanceof String) {
            return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                .setStringValue((String) val)
                .build();
        } else if (val instanceof Boolean) {
            return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                .setBoolValue((Boolean) val)
                .build();
        } else if (val instanceof Number) {
            if (val instanceof Integer || val instanceof Long) {
                return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                    .setIntValue(((Number) val).longValue())
                    .build();
            } else {
                return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                    .setDoubleValue(((Number) val).doubleValue())
                    .build();
            }
        } else {
            return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                .setStringValue(val.toString())
                .build();
        }
    }
    
    private byte[] hexToBytes(String hex) {
        int length = hex.length();
        byte[] data = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}

