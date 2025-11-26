package ai.softprobe.otel.controller;

import ai.softprobe.otel.model.AcceptOtelTracesResponse;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.resource.v1.Resource;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import ai.softprobe.otel.repository.LogRepository;
import ai.softprobe.otel.repository.MetricRepository;
import ai.softprobe.otel.service.TraceProcessingService;
import ai.softprobe.otel.util.OtlpFormatUtils;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@Tag(name = "OTLP Ingestion", description = "OpenTelemetry Protocol (OTLP) ingestion endpoints for receiving telemetry data in both protobuf and JSON formats")
public class OtelIngesterController {

    @Autowired
    private LogRepository logRepository;

    @Autowired
    private MetricRepository metricRepository;

    @Autowired
    private TraceProcessingService traceProcessingService;

    @Operation(
            summary = "Ingest OpenTelemetry logs",
            description = "Accepts OpenTelemetry logs in both protobuf and JSON formats. Use Content-Type header to specify format: 'application/x-protobuf' for binary protobuf or 'application/json' for JSON."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Logs ingested successfully"),
            @ApiResponse(responseCode = "400", description = "Bad request - invalid data format"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @RequestBody(
            description = "OTLP logs data",
            content = {
                    @Content(mediaType = "application/x-protobuf", schema = @Schema(type = "string", format = "binary")),
                    @Content(mediaType = "application/json", examples = @ExampleObject(value = "{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"my-service\"}}]},\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"1600000000000000000\",\"body\":{\"stringValue\":\"Log message\"}}]}]}]}"))
            }
    )
    @PostMapping(value = "/v1/logs", consumes = {"application/x-protobuf", "application/json"})
    public ResponseEntity<String> acceptOtelLogs(@org.springframework.web.bind.annotation.RequestBody byte[] body, @RequestHeader("Content-Type") String contentType) {
        try {
            ExportLogsServiceRequest request = OtlpFormatUtils.parseLogsRequest(body, contentType);
            List<io.opentelemetry.proto.logs.v1.LogRecord> logRecords = new ArrayList<>();
            Resource resource = null;

            for (var resourceLogs : request.getResourceLogsList()) {
                resource = resourceLogs.getResource();
                for (var scopeLogs : resourceLogs.getScopeLogsList()) {
                    logRecords.addAll(scopeLogs.getLogRecordsList());
                }
            }

            if (!logRecords.isEmpty() && resource != null) {
                logRepository.storeLogs(logRecords, resource);
                log.info("Ingested {} logs successfully", logRecords.size());
                return ResponseEntity.ok("Logs ingested successfully");
            } else {
                log.warn("No logs to ingest");
                return ResponseEntity.ok("No logs to ingest");
            }
        } catch (IllegalArgumentException e) {
            log.error("Bad request: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Bad request: " + e.getMessage());
        } catch (Exception e) {
            log.error("Error processing OTLP request: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing OTLP request: " + e.getMessage());
        }
    }

    @Operation(
            summary = "Ingest OpenTelemetry metrics",
            description = "Accepts OpenTelemetry metrics in both protobuf and JSON formats. Use Content-Type header to specify format: 'application/x-protobuf' for binary protobuf or 'application/json' for JSON."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Metrics ingested successfully"),
            @ApiResponse(responseCode = "400", description = "Bad request - invalid data format"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @RequestBody(
            description = "OTLP metrics data",
            content = {
                    @Content(mediaType = "application/x-protobuf", schema = @Schema(type = "string", format = "binary")),
                    @Content(mediaType = "application/json", examples = @ExampleObject(value = "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"my-service\"}}]},\"scopeMetrics\":[{\"metrics\":[{\"name\":\"cpu_usage\",\"gauge\":{\"dataPoints\":[{\"asDouble\":0.75,\"timeUnixNano\":\"1600000000000000000\"}]}}]}]}]}"))
            }
    )
    @PostMapping(value = "/v1/metrics", consumes = {"application/x-protobuf", "application/json"})
    public ResponseEntity<String> acceptOtelMetrics(@org.springframework.web.bind.annotation.RequestBody byte[] body, @RequestHeader("Content-Type") String contentType) {
        try {
            ExportMetricsServiceRequest request = OtlpFormatUtils.parseMetricsRequest(body, contentType);
            List<Metric> metrics = new ArrayList<>();
            Resource resource = null;

            for (var resourceMetrics : request.getResourceMetricsList()) {
                resource = resourceMetrics.getResource();
                for (var scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                    metrics.addAll(scopeMetrics.getMetricsList());
                }
            }

            if (!metrics.isEmpty() && resource != null) {
                metricRepository.storeMetrics(metrics, resource);
                log.info("Ingested {} metrics successfully", metrics.size());
                return ResponseEntity.ok("Metrics ingested successfully");
            } else {
                log.warn("No metrics to ingest");
                return ResponseEntity.ok("No metrics to ingest");
            }
        } catch (IllegalArgumentException e) {
            log.error("Bad request: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Bad request: " + e.getMessage());
        } catch (Exception e) {
            log.error("Error processing OTLP request: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing OTLP request: " + e.getMessage());
        }
    }

    @Operation(
            summary = "Ingest OpenTelemetry traces",
            description = "Accepts OpenTelemetry traces in both protobuf and JSON formats. Use Content-Type header to specify format: 'application/x-protobuf' for binary protobuf or 'application/json' for JSON."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Traces ingested successfully"),
            @ApiResponse(responseCode = "400", description = "Bad request - invalid data format"),
            @ApiResponse(responseCode = "404", description = "No traces to ingest"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @RequestBody(
            description = "OTLP traces data",
            content = {
                    @Content(mediaType = "application/x-protobuf", schema = @Schema(type = "string", format = "binary")),
                    @Content(mediaType = "application/json", examples = @ExampleObject(value = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"my-service\"}}]},\"scopeSpans\":[{\"spans\":[{\"traceId\":\"5b8efff798038103d269b633813fc60c\",\"spanId\":\"051581bf3cb55c13\",\"name\":\"HTTP GET\",\"kind\":2,\"startTimeUnixNano\":\"1600000000000000000\",\"endTimeUnixNano\":\"1600000001000000000\",\"status\":{\"code\":1}}]}]}]}"))
            }
    )
    @PostMapping(value = "/v1/traces", consumes = {"application/x-protobuf", "application/json"})
    public ResponseEntity<AcceptOtelTracesResponse> acceptOtelTraces(@org.springframework.web.bind.annotation.RequestBody byte[] body, @RequestHeader("Content-Type") String contentType) {
        AcceptOtelTracesResponse response = new AcceptOtelTracesResponse();
        try {
            traceProcessingService.processTraces(body, contentType);
            response.setMessage("Traces ingested successfully");
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            log.error("Bad request: {}", e.getMessage(), e);
            response.setMessage("Bad request: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        } catch (Exception e) {
            log.error("Error processing OTLP request: {}", e.getMessage(), e);
            response.setMessage("Error processing OTLP request: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}

