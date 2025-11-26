package ai.softprobe.otel.service;

import ai.softprobe.otel.model.*;
import ai.softprobe.otel.model.SessionInfo;
import ai.softprobe.otel.repository.QueryRepository;
import ai.softprobe.otel.config.ClickHouseProperties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TraceViewService {

    private final QueryRepository queryRepository;
    private final DataSource clickHouseDataSource;
    private final ClickHouseProperties properties;
    private final Gson gson;

    @Autowired
    public TraceViewService(QueryRepository queryRepository, DataSource clickHouseDataSource, ClickHouseProperties properties) {
        this.queryRepository = queryRepository;
        this.clickHouseDataSource = clickHouseDataSource;
        this.properties = properties;
        this.gson = new GsonBuilder()
                .registerTypeAdapter(Instant.class, new InstantTypeAdapter())
                .create();
    }

    // Custom TypeAdapter for Instant to handle JSON serialization
    private static class InstantTypeAdapter extends TypeAdapter<Instant> {
        @Override
        public void write(JsonWriter out, Instant value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.toString());
            }
        }

        @Override
        public Instant read(JsonReader in) throws IOException {
            if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            return Instant.parse(in.nextString());
        }
    }

    public TraceViewResponse getTraceView(TraceViewRequest request) {
        try {
            log.info("Getting trace view for traceId: {}", request.getTraceId());

            String traceIdHex = convertTraceIdToHex(request.getTraceId());
            log.info("Converted traceId: {} -> {}", request.getTraceId(), traceIdHex);

            TraceQueryFilter filter = TraceQueryFilter.builder()
                    .traceId(traceIdHex)
                    .limit(1000)
                    .offset(0)
                    .build();

            ExportTraceServiceRequest spanData = queryRepository.queryTraces(filter);
            List<TraceSpanNode> allSpans = convertToSpanNodes(spanData);
            List<TraceSpanNode> treeRoots = buildSpanTree(allSpans);

            long totalSpans = allSpans.size();
            long totalDuration = calculateTotalDuration(allSpans);

            return TraceViewResponse.builder()
                    .success(true)
                    .traceId(request.getTraceId())
                    .spans(treeRoots)
                    .totalSpans(totalSpans)
                    .totalDuration(totalDuration)
                    .build();

        } catch (Exception e) {
            log.error("Error getting trace view for traceId: {}", request.getTraceId(), e);
            return TraceViewResponse.builder()
                    .success(false)
                    .message("Error retrieving trace: " + e.getMessage())
                    .traceId(request.getTraceId())
                    .build();
        }
    }

    public TraceDetailResponse getTraceDetail(TraceDetailRequest request) {
        try {
            log.info("Getting trace detail for traceId: {}, spanId: {}", request.getTraceId(), request.getSpanId());

            String traceIdHex = convertTraceIdToHex(request.getTraceId());
            String spanIdHex = convertTraceIdToHex(request.getSpanId());

            SessionTraceResponse.SpanInfo spanDetails;
            try {
                spanDetails = getSpanDetails(traceIdHex, spanIdHex);
            } catch (SQLException e) {
                log.error("Failed to get span details: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to query span details", e);
            }
            if (spanDetails == null) {
                return TraceDetailResponse.builder()
                        .success(false)
                        .message("Span not found")
                        .traceId(request.getTraceId())
                        .spanId(request.getSpanId())
                        .build();
            }

            List<TraceDetailResponse.LogEntry> requestLogs = getLogsForSpan(traceIdHex, spanIdHex, "REQUEST");
            List<TraceDetailResponse.LogEntry> responseLogs = getLogsForSpan(traceIdHex, spanIdHex, "RESPONSE");

            return TraceDetailResponse.builder()
                    .success(true)
                    .traceId(spanDetails.getTraceId())
                    .spanId(spanDetails.getSpanId())
                    .parentSpanId(spanDetails.getParentSpanId())
                    .name(spanDetails.getName())
                    .kind(spanDetails.getKind())
                    .startTime(spanDetails.getStartTime())
                    .endTime(spanDetails.getEndTime())
                    .duration(spanDetails.getDuration())
                    .status(spanDetails.getStatus())
                    .attributes(spanDetails.getAttributes())
                    .serviceName(spanDetails.getServiceName())
                    .spanType(spanDetails.getSpanType())
                    .requestLogs(requestLogs)
                    .responseLogs(responseLogs)
                    .build();

        } catch (Exception e) {
            log.error("Error getting trace detail for traceId: {}, spanId: {}", request.getTraceId(), request.getSpanId(), e);
            return TraceDetailResponse.builder()
                    .success(false)
                    .message("Error retrieving trace detail: " + e.getMessage())
                    .traceId(request.getTraceId())
                    .spanId(request.getSpanId())
                    .build();
        }
    }

    public SessionListResponse getAllSessionIds(int size, String serviceName, Integer httpStatusCode, String startTimeFrom, String startTimeTo, String cursor) {
        try {
            log.info("Getting session IDs with filters - size: {}, serviceName: {}, httpStatusCode: {}, startTimeFrom: {}, startTimeTo: {}",
                    size, serviceName, httpStatusCode, startTimeFrom, startTimeTo);

            StringBuilder whereClause = new StringBuilder("WHERE session_id IS NOT NULL AND session_id != ''");
            List<Object> params = new ArrayList<>();

            if (serviceName != null && !serviceName.trim().isEmpty()) {
                whereClause.append(" AND service_name = ?");
                params.add(serviceName);
            }

            if (httpStatusCode != null) {
                whereClause.append(" AND http_status_code = ?");
                params.add(httpStatusCode);
            }

            if (startTimeFrom != null && !startTimeFrom.trim().isEmpty()) {
                whereClause.append(" AND start_time >= ?");
                params.add(Timestamp.valueOf(startTimeFrom.replace("T", " ").replace("Z", "")));
            }

            if (startTimeTo != null && !startTimeTo.trim().isEmpty()) {
                whereClause.append(" AND start_time <= ?");
                params.add(Timestamp.valueOf(startTimeTo.replace("T", " ").replace("Z", "")));
            }

            long rowNumOffset = 0;
            if (cursor != null && !cursor.trim().isEmpty()) {
                try {
                    rowNumOffset = Long.parseLong(cursor);
                } catch (NumberFormatException e) {
                    log.warn("Invalid cursor format: {}, using default offset 0", cursor);
                }
            }

            // Get total count
            String countSql = "SELECT uniqExact(session_id) as total_count " +
                    "FROM " + properties.getDatabase() + "." + properties.getEnvoyProxySpansTableId() + " " +
                    whereClause.toString();

            long totalCount = 0;
            try (Connection conn = clickHouseDataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(countSql)) {
                setParameters(pstmt, params);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        totalCount = rs.getLong("total_count");
                    }
                }
            }

            // Get sessions with pagination
            String sql = "SELECT session_id, span_id, created_at, service_name, http_status_code " +
                    "FROM " + properties.getDatabase() + "." + properties.getEnvoyProxySpansTableId() + " " +
                    whereClause.toString() + " " +
                    "ORDER BY created_at DESC " +
                    "LIMIT ? OFFSET ?";

            List<SessionInfo> sessions = new ArrayList<>();
            Map<String, SessionInfo> sessionMap = new LinkedHashMap<>();
            String nextCursor = null;

            try (Connection conn = clickHouseDataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                setParameters(pstmt, params);
                pstmt.setInt(params.size() + 1, size * 5); // Allow for multiple spans per session
                pstmt.setLong(params.size() + 2, rowNumOffset);

                try (ResultSet rs = pstmt.executeQuery()) {
                    long currentRow = rowNumOffset;
                    while (rs.next() && sessions.size() < size) {
                        currentRow++;
                        String sessionId = rs.getString("session_id");
                        if (sessionId == null || sessionId.trim().isEmpty()) {
                            continue;
                        }

                        Timestamp createdAt = rs.getTimestamp("created_at");
                        Instant createdAtInstant = createdAt != null ? createdAt.toInstant() : Instant.now();

                        SessionInfo sessionInfo = sessionMap.computeIfAbsent(sessionId, id -> {
                            try {
                                SessionInfo.SessionInfoBuilder builder = SessionInfo.builder()
                                        .sessionId(id)
                                        .createdAt(createdAtInstant)
                                        .spanIds(new ArrayList<>());

                                String svcName = rs.getString("service_name");
                                if (svcName != null) {
                                    builder.serviceName(svcName);
                                }

                                Integer statusCode = rs.getObject("http_status_code", Integer.class);
                                if (statusCode != null) {
                                    builder.httpStatusCode(String.valueOf(statusCode));
                                }

                                return builder.build();
                            } catch (SQLException e) {
                                log.error("Error building session info: {}", e.getMessage(), e);
                                return SessionInfo.builder()
                                        .sessionId(id)
                                        .createdAt(createdAtInstant)
                                        .spanIds(new ArrayList<>())
                                        .build();
                            }
                        });

                        String spanId = rs.getString("span_id");
                        if (spanId != null && !spanId.trim().isEmpty()) {
                            sessionInfo.getSpanIds().add(spanId);
                        }

                        if (sessions.size() == size - 1) {
                            nextCursor = String.valueOf(currentRow);
                        }
                    }
                }
            }

            sessions.addAll(sessionMap.values());
            if (sessions.size() > size) {
                sessions = sessions.subList(0, size);
            }

            return SessionListResponse.builder()
                    .success(true)
                    .sessions(sessions)
                    .totalCount(totalCount)
                    .pageSize(size)
                    .nextCursor(nextCursor)
                    .hasMore(nextCursor != null)
                    .createdAt(Instant.now())
                    .build();

        } catch (Exception e) {
            log.error("Error getting session IDs with filters", e);
            return SessionListResponse.builder()
                    .success(false)
                    .message("Error retrieving session IDs: " + e.getMessage())
                    .sessions(new ArrayList<>())
                    .totalCount(0)
                    .createdAt(Instant.now())
                    .build();
        }
    }

    public SessionTraceResponse getTracesBySessionId(String sessionId) {
        try {
            log.info("Getting traces and spans for sessionId: {}", sessionId);

            Set<String> traceIds;
            try {
                traceIds = getTraceIdsBySessionId(sessionId);
            } catch (SQLException e) {
                log.error("Failed to get trace IDs for session: {}", sessionId, e);
                throw new RuntimeException("Failed to query trace IDs", e);
            }
            if (traceIds.isEmpty()) {
                return SessionTraceResponse.builder()
                        .success(true)
                        .sessionId(sessionId)
                        .traces(new ArrayList<>())
                        .totalTraces(0)
                        .totalSpans(0)
                        .build();
            }

            List<SessionTraceResponse.TraceInfo> traces = new ArrayList<>();
            int totalSpans = 0;

            for (String traceId : traceIds) {
                List<SessionTraceResponse.SpanInfo> allSpans;
                try {
                    allSpans = getSpansByTraceId(traceId);
                } catch (SQLException e) {
                    log.error("Failed to get spans for trace: {}", traceId, e);
                    continue;
                }
                if (!allSpans.isEmpty()) {
                    List<SessionTraceResponse.SpanInfo> filteredSpans = filterEnvoyProxySpans(allSpans);
                    if (!filteredSpans.isEmpty()) {
                        List<SessionTraceResponse.SpanInfo> hierarchicalSpans = buildSpanHierarchy(filteredSpans);
                        long traceDuration = calculateTraceDuration(hierarchicalSpans);

                        SessionTraceResponse.TraceInfo traceInfo = SessionTraceResponse.TraceInfo.builder()
                                .traceId(traceId)
                                .spans(hierarchicalSpans)
                                .totalDuration(traceDuration)
                                .build();

                        traces.add(traceInfo);
                        totalSpans += hierarchicalSpans.size();
                    }
                }
            }

            return SessionTraceResponse.builder()
                    .success(true)
                    .sessionId(sessionId)
                    .traces(traces)
                    .totalTraces(traces.size())
                    .totalSpans(totalSpans)
                    .build();

        } catch (Exception e) {
            log.error("Error getting traces for session ID: {}", sessionId, e);
            return SessionTraceResponse.builder()
                    .success(false)
                    .message("Error retrieving traces: " + e.getMessage())
                    .sessionId(sessionId)
                    .traces(new ArrayList<>())
                    .totalTraces(0)
                    .totalSpans(0)
                    .build();
        }
    }

    private Set<String> getTraceIdsBySessionId(String sessionId) throws SQLException {
        String sql = "SELECT DISTINCT trace_id " +
                "FROM " + properties.getDatabase() + "." + properties.getEnvoyProxySpansTableId() + " " +
                "WHERE session_id = ?";

        Set<String> traceIds = new HashSet<>();
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, sessionId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    traceIds.add(rs.getString("trace_id"));
                }
            }
        }
        return traceIds;
    }

    private List<SessionTraceResponse.SpanInfo> getSpansByTraceId(String traceId) throws SQLException {
        String sql = "SELECT trace_id, span_id, parent_span_id, name, start_time, end_time, " +
                "kind, status, resource, attributes, span_type " +
                "FROM " + properties.getDatabase() + "." + properties.getSpansTableId() + " " +
                "WHERE trace_id = ? " +
                "ORDER BY start_time ASC";

        List<SessionTraceResponse.SpanInfo> spans = new ArrayList<>();
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, traceId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    spans.add(convertToSpanInfo(rs));
                }
            }
        }
        return spans;
    }

    private SessionTraceResponse.SpanInfo convertToSpanInfo(ResultSet rs) throws SQLException {
        String traceId = rs.getString("trace_id");
        String spanId = rs.getString("span_id");
        String parentSpanId = rs.getString("parent_span_id");
        String name = rs.getString("name");

        Timestamp startTime = rs.getTimestamp("start_time");
        Timestamp endTime = rs.getTimestamp("end_time");
        Instant startTimeInstant = startTime != null ? startTime.toInstant() : Instant.now();
        Instant endTimeInstant = endTime != null ? endTime.toInstant() : Instant.now();
        long duration = Duration.between(startTimeInstant, endTimeInstant).toMillis();

        String attributesJson = rs.getString("attributes");
        String kind = rs.getString("kind");
        String status = rs.getString("status");
        String resourceJson = rs.getString("resource");
        String spanType = rs.getString("span_type");

        String serviceName = extractServiceNameFromJson(resourceJson);
        Map<String, Object> attributes = parseAttributesJson(attributesJson != null ? attributesJson : "{}");

        return SessionTraceResponse.SpanInfo.builder()
                .traceId(traceId)
                .spanId(spanId)
                .parentSpanId(parentSpanId)
                .name(name)
                .kind(kind != null ? kind : "SERVER")
                .startTime(startTimeInstant)
                .endTime(endTimeInstant)
                .duration(duration)
                .status(status != null ? status : "OK")
                .attributes(attributes)
                .serviceName(serviceName)
                .spanType(spanType)
                .requestLogs(new ArrayList<>())
                .responseLogs(new ArrayList<>())
                .children(new ArrayList<>())
                .build();
    }

    private SessionTraceResponse.SpanInfo getSpanDetails(String traceId, String spanId) throws SQLException {
        String sql = "SELECT trace_id, span_id, parent_span_id, name, start_time, end_time, " +
                "kind, status, resource, attributes, span_type " +
                "FROM " + properties.getDatabase() + "." + properties.getSpansTableId() + " " +
                "WHERE trace_id = ? AND span_id = ?";

        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, traceId);
            pstmt.setString(2, spanId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return convertToSpanInfo(rs);
                }
            }
        }
        return null;
    }

    private List<SessionTraceResponse.SpanInfo> filterEnvoyProxySpans(List<SessionTraceResponse.SpanInfo> spans) {
        return spans.stream()
                .filter(span -> "sp-envoy-proxy".equals(span.getSpanType()))
                .collect(Collectors.toList());
    }

    private List<SessionTraceResponse.SpanInfo> buildSpanHierarchy(List<SessionTraceResponse.SpanInfo> spans) {
        if (spans == null || spans.isEmpty()) {
            return new ArrayList<>();
        }

        Map<String, SessionTraceResponse.SpanInfo> spanMap = new HashMap<>();
        List<SessionTraceResponse.SpanInfo> rootSpans = new ArrayList<>();
        Set<String> childSpanIds = new HashSet<>();

        for (SessionTraceResponse.SpanInfo span : spans) {
            if (span != null && span.getSpanId() != null) {
                spanMap.put(span.getSpanId(), span);
            }
        }

        for (SessionTraceResponse.SpanInfo span : spans) {
            if (span == null || span.getSpanId() == null) {
                continue;
            }

            if (span.getParentSpanId() == null || span.getParentSpanId().isEmpty()) {
                rootSpans.add(span);
            } else {
                SessionTraceResponse.SpanInfo parent = spanMap.get(span.getParentSpanId());
                if (parent != null && parent.getChildren() != null) {
                    parent.getChildren().add(span);
                    childSpanIds.add(span.getSpanId());
                } else {
                    rootSpans.add(span);
                }
            }
        }

        rootSpans.removeIf(span -> span != null && childSpanIds.contains(span.getSpanId()));
        return rootSpans;
    }

    private long calculateTraceDuration(List<SessionTraceResponse.SpanInfo> spans) {
        if (spans == null || spans.isEmpty()) {
            return 0;
        }

        Instant earliestStart = spans.stream()
                .map(SessionTraceResponse.SpanInfo::getStartTime)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(Instant.now());

        Instant latestEnd = spans.stream()
                .map(SessionTraceResponse.SpanInfo::getEndTime)
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        return Duration.between(earliestStart, latestEnd).toMillis();
    }

    private List<TraceSpanNode> convertToSpanNodes(ExportTraceServiceRequest spanData) {
        List<TraceSpanNode> nodes = new ArrayList<>();

        for (ResourceSpans resourceSpans : spanData.getResourceSpansList()) {
            String serviceName = extractServiceName(resourceSpans.getResource());

            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                for (Span span : scopeSpans.getSpansList()) {
                    TraceSpanNode node = convertSpanToNode(span, serviceName);
                    nodes.add(node);
                }
            }
        }

        return nodes;
    }

    private TraceSpanNode convertSpanToNode(Span span, String serviceName) {
        String spanIdHex = bytesToHex(span.getSpanId().toByteArray());
        String parentSpanIdHex = span.getParentSpanId().isEmpty() ? null : bytesToHex(span.getParentSpanId().toByteArray());
        String traceIdHex = bytesToHex(span.getTraceId().toByteArray());

        Instant startTime = Instant.ofEpochSecond(0, span.getStartTimeUnixNano());
        Instant endTime = Instant.ofEpochSecond(0, span.getEndTimeUnixNano());
        long duration = endTime.toEpochMilli() - startTime.toEpochMilli();

        Map<String, Object> attributes = convertAttributesToMap(span.getAttributesList());

        return TraceSpanNode.builder()
                .spanId(spanIdHex)
                .parentSpanId(parentSpanIdHex)
                .traceId(traceIdHex)
                .name(span.getName())
                .kind(span.getKind().name())
                .startTime(startTime)
                .endTime(endTime)
                .duration(duration)
                .status(span.getStatus().getCode().name())
                .attributes(attributes)
                .serviceName(serviceName)
                .children(new ArrayList<>())
                .build();
    }

    private List<TraceSpanNode> buildSpanTree(List<TraceSpanNode> allSpans) {
        Map<String, TraceSpanNode> spanMap = allSpans.stream()
                .collect(Collectors.toMap(TraceSpanNode::getSpanId, span -> span));

        List<TraceSpanNode> roots = new ArrayList<>();

        for (TraceSpanNode span : allSpans) {
            if (span.getParentSpanId() == null || span.getParentSpanId().isEmpty()) {
                roots.add(span);
            } else {
                TraceSpanNode parent = spanMap.get(span.getParentSpanId());
                if (parent != null) {
                    parent.getChildren().add(span);
                } else {
                    roots.add(span);
                }
            }
        }

        return roots;
    }

    private long calculateTotalDuration(List<TraceSpanNode> allSpans) {
        if (allSpans == null || allSpans.isEmpty()) {
            return 0;
        }

        Instant earliestStart = allSpans.stream()
                .map(TraceSpanNode::getStartTime)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(Instant.now());

        Instant latestEnd = allSpans.stream()
                .map(TraceSpanNode::getEndTime)
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        return Duration.between(earliestStart, latestEnd).toMillis();
    }

    private String extractServiceName(io.opentelemetry.proto.resource.v1.Resource resource) {
        for (io.opentelemetry.proto.common.v1.KeyValue attribute : resource.getAttributesList()) {
            if ("service.name".equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                return attribute.getValue().getStringValue();
            }
        }
        return "unknown";
    }

    private String extractServiceNameFromJson(String resourceJson) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resourceMap = gson.fromJson(resourceJson, Map.class);
            if (resourceMap != null) {
                Object serviceName = resourceMap.get("service_name");
                if (serviceName == null) {
                    serviceName = resourceMap.get("service.name");
                }
                return serviceName != null ? serviceName.toString() : "unknown";
            }
        } catch (Exception e) {
            log.warn("Failed to extract service name from JSON: {}", resourceJson, e);
        }
        return "unknown";
    }

    private Map<String, Object> parseAttributesJson(String attributesJson) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> attributes = gson.fromJson(attributesJson, Map.class);
            return attributes != null ? attributes : new HashMap<>();
        } catch (Exception e) {
            log.warn("Failed to parse attributes JSON: {}", attributesJson, e);
            return new HashMap<>();
        }
    }

    private Map<String, Object> convertAttributesToMap(List<io.opentelemetry.proto.common.v1.KeyValue> attributesList) {
        Map<String, Object> result = new HashMap<>();
        for (io.opentelemetry.proto.common.v1.KeyValue kv : attributesList) {
            result.put(kv.getKey(), transformAnyValue(kv.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unused")
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
        }
        return null;
    }

    private List<TraceDetailResponse.LogEntry> getLogsForSpan(String traceId, String spanId, String type) {
        // Simplified - can be enhanced to query logs table
        return new ArrayList<>();
    }

    private String convertTraceIdToHex(String traceId) {
        try {
            // Try to decode as base64 first
            byte[] decoded = java.util.Base64.getDecoder().decode(traceId);
            return bytesToHex(decoded);
        } catch (Exception e) {
            // If not base64, assume it's already hex
            return traceId;
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private void setParameters(PreparedStatement pstmt, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object param = params.get(i);
            if (param instanceof String) {
                pstmt.setString(i + 1, (String) param);
            } else if (param instanceof Integer) {
                pstmt.setInt(i + 1, (Integer) param);
            } else if (param instanceof Long) {
                pstmt.setLong(i + 1, (Long) param);
            } else if (param instanceof Timestamp) {
                pstmt.setTimestamp(i + 1, (Timestamp) param);
            }
        }
    }
}

