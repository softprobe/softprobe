package ai.softprobe.otel.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.Map;

@RestController
@Tag(name = "Health", description = "Application health check endpoint")
public class HealthController {
    
    @Operation(
        summary = "Health check",
        description = "Check the health status of the API"
    )
    @ApiResponse(responseCode = "200", description = "API is healthy")
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "message", "OpenTelemetry Query API is running"
        ));
    }
}

