package ai.softprobe.otel.controller;

import ai.softprobe.otel.model.*;
import ai.softprobe.otel.service.TraceViewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.time.Instant;
import java.util.ArrayList;

@Slf4j
@RestController
@RequestMapping("/api")
@Tag(name = "Trace Visualization", description = "Trace visualization and session management endpoints")
public class TraceViewController {

    private final TraceViewService traceViewService;

    @Autowired
    public TraceViewController(TraceViewService traceViewService) {
        this.traceViewService = traceViewService;
    }

    @Operation(
        summary = "Get trace view",
        description = "Retrieve a hierarchical view of spans for a given trace ID"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Trace view retrieved successfully",
                    content = @Content(schema = @Schema(implementation = TraceViewResponse.class))),
        @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = TraceViewResponse.class)))
    })
    @Deprecated
    @GetMapping("/view")
    public ResponseEntity<TraceViewResponse> getTraceView(
            @Parameter(description = "The trace ID to retrieve", required = true) @RequestParam String traceId) {
        try {
            log.info("Getting trace view for traceId: {}", traceId);
            TraceViewRequest request = TraceViewRequest.builder()
                .traceId(traceId)
                .build();

            TraceViewResponse response = traceViewService.getTraceView(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting trace view for traceId: {}", traceId, e);
            return ResponseEntity.internalServerError()
                .body(TraceViewResponse.builder()
                    .success(false)
                    .message("Error retrieving trace: " + e.getMessage())
                    .build());
        }
    }

    @Operation(
        summary = "Get trace detail",
        description = "Retrieve detailed information for a specific span within a trace"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Trace detail retrieved successfully",
                    content = @Content(schema = @Schema(implementation = TraceDetailResponse.class))),
        @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = TraceDetailResponse.class)))
    })
    @GetMapping("/detail")
    public ResponseEntity<TraceDetailResponse> getTraceDetail(
            @Parameter(description = "The trace ID", required = true) @RequestParam String traceId,
            @Parameter(description = "The span ID within the trace", required = true) @RequestParam String spanId) {
        try {
            log.info("Getting trace detail for traceId: {}, spanId: {}", traceId, spanId);
            TraceDetailRequest request = TraceDetailRequest.builder()
                .traceId(traceId)
                .spanId(spanId)
                .build();

            TraceDetailResponse response = traceViewService.getTraceDetail(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting trace detail for traceId: {}, spanId: {}", traceId, spanId, e);
            return ResponseEntity.internalServerError()
                .body(TraceDetailResponse.builder()
                    .success(false)
                    .message("Error retrieving trace detail: " + e.getMessage())
                    .build());
        }
    }

    @Operation(
        summary = "Get all session IDs",
        description = "Retrieve a list of all available session IDs"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Session IDs retrieved successfully",
                    content = @Content(schema = @Schema(implementation = SessionListResponse.class))),
        @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = SessionListResponse.class)))
    })
    @GetMapping("/sessions")
    public ResponseEntity<SessionListResponse> getAllSessionIds(
            @Parameter(description = "Page size", required = false) @RequestParam(defaultValue = "20") int size,
            @Parameter(description = "Filter by service name", required = false) @RequestParam(required = false) String serviceName,
            @Parameter(description = "Filter by HTTP status code", required = false) @RequestParam(required = false) Integer httpStatusCode,
            @Parameter(description = "Filter by start time (ISO format)", required = false) @RequestParam(required = false) String startTimeFrom,
            @Parameter(description = "Filter by end time (ISO format)", required = false) @RequestParam(required = false) String startTimeTo,
            @Parameter(description = "Cursor for pagination", required = false) @RequestParam(required = false) String cursor) {
        try {
            log.info("Getting session IDs with filters - size: {}, serviceName: {}, httpStatusCode: {}, startTimeFrom: {}, startTimeTo: {}", 
                    size, serviceName, httpStatusCode, startTimeFrom, startTimeTo);
            
            SessionListResponse response = traceViewService.getAllSessionIds(size, serviceName, httpStatusCode, startTimeFrom, startTimeTo, cursor);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting session IDs", e);
            return ResponseEntity.internalServerError()
                .body(SessionListResponse.builder()
                    .success(false)
                    .message("Error retrieving session IDs: " + e.getMessage())
                    .sessions(new ArrayList<>())
                    .totalCount(0)
                    .createdAt(Instant.now())
                    .build());
        }
    }

    @Operation(
        summary = "Get traces by session ID",
        description = "Retrieve all traces associated with a specific session ID"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Traces retrieved successfully",
                    content = @Content(schema = @Schema(implementation = SessionTraceResponse.class))),
        @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = SessionTraceResponse.class)))
    })
    @GetMapping("/sessions/{sessionId}")
    public ResponseEntity<SessionTraceResponse> getTracesBySessionId(
            @Parameter(description = "The session ID to retrieve traces for", required = true) @PathVariable String sessionId) {
        try {
            log.info("Getting traces for sessionId: {}", sessionId);
            SessionTraceResponse response = traceViewService.getTracesBySessionId(sessionId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting traces for sessionId: {}", sessionId, e);
            return ResponseEntity.internalServerError()
                .body(SessionTraceResponse.builder()
                    .success(false)
                    .message("Error retrieving traces: " + e.getMessage())
                    .sessionId(sessionId)
                    .traces(new ArrayList<>())
                    .totalTraces(0)
                    .totalSpans(0)
                    .build());
        }
    }
}

