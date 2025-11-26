package ai.softprobe.otel.util;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.common.v1.KeyValue;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class OtlpFormatUtils {
    
    private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser()
        .ignoringUnknownFields(); // OTLP compliance: accept unknown fields
    
    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer()
        .omittingInsignificantWhitespace()
        .printingEnumsAsInts(); // OTLP compliance: enum values as integers
    
    public static ExportTraceServiceRequest parseTraceRequest(byte[] body, String contentType) {
        return parseRequest(body, contentType, 
            ExportTraceServiceRequest::parseFrom, 
            ExportTraceServiceRequest::newBuilder);
    }
    
    public static ExportMetricsServiceRequest parseMetricsRequest(byte[] body, String contentType) {
        return parseRequest(body, contentType, 
            ExportMetricsServiceRequest::parseFrom, 
            ExportMetricsServiceRequest::newBuilder);
    }
    
    public static ExportLogsServiceRequest parseLogsRequest(byte[] body, String contentType) {
        return parseRequest(body, contentType, 
            ExportLogsServiceRequest::parseFrom, 
            ExportLogsServiceRequest::newBuilder);
    }
    
    @SuppressWarnings("unchecked")
    private static <T extends Message> T parseRequest(byte[] body, String contentType, 
            ProtobufParser<T> protobufParser, BuilderSupplier<T> builderSupplier) {
        try {
            if (isProtobufContent(contentType)) {
                return protobufParser.parseFrom(body);
            } else {
                String jsonBody = new String(body, StandardCharsets.UTF_8);
                Message.Builder builder = builderSupplier.newBuilder();
                JSON_PARSER.merge(jsonBody, builder);
                return (T) builder.build();
            }
        } catch (InvalidProtocolBufferException e) {
            // This covers both protobuf parsing errors and JSON parsing errors
            // since JSON parsing also throws InvalidProtocolBufferException
            if (isProtobufContent(contentType)) {
                throw new IllegalArgumentException("Invalid protobuf data: " + e.getMessage(), e);
            } else {
                throw new IllegalArgumentException("Invalid JSON format: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse request: " + e.getMessage(), e);
        }
    }
    
    @FunctionalInterface
    private interface ProtobufParser<T> {
        T parseFrom(byte[] data) throws InvalidProtocolBufferException;
    }
    
    @FunctionalInterface
    private interface BuilderSupplier<T> {
        Message.Builder newBuilder();
    }
    
    public static ResponseEntity<?> formatResponse(Message message, String acceptHeader) throws IOException {
        if (isJsonAccepted(acceptHeader)) {
            String jsonResponse = JSON_PRINTER.print(message);
            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonResponse);
        } else {
            return ResponseEntity.ok()
                .contentType(MediaType.valueOf("application/x-protobuf"))
                .body(message);
        }
    }
    
    public static boolean isProtobufContent(String contentType) {
        return contentType != null && contentType.toLowerCase().contains("protobuf");
    }
    
    public static boolean isJsonAccepted(String acceptHeader) {
        return acceptHeader != null && acceptHeader.toLowerCase().contains("json");
    }
    
    /**
     * 从 span attributes 中提取 sp.api.name 值
     * @param spans span 列表
     * @return sp.api.name 的值，如果未找到则返回 null
     */
    public static String extractApiName(List<Span> spans) {
        if (spans == null || spans.isEmpty()) {
            return null;
        }
        
        for (Span span : spans) {
            String apiName = extractApiNameFromSpan(span);
            if (apiName != null) {
                return apiName;
            }
        }
        
        return null;
    }
    
    /**
     * 从单个 span 的 attributes 中提取 sp.api.name 值
     * @param span 单个 span
     * @return sp.api.name 的值，如果未找到则返回 null
     */
    public static String extractApiNameFromSpan(Span span) {
        if (span == null || span.getAttributesList().isEmpty()) {
            return null;
        }
        
        for (KeyValue attribute : span.getAttributesList()) {
            if ("sp.api.name".equals(attribute.getKey())) {
                if (attribute.getValue().hasStringValue()) {
                    return attribute.getValue().getStringValue();
                }
            }
        }
        
        return null;
    }
}

