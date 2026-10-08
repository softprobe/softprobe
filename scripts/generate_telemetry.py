#!/usr/bin/env python3
"""
Generate OTLP telemetry data with sp.session.id attribute.
Sends traces to the OTLP backend via HTTP/Protobuf.
"""

import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import Status, StatusCode

# Configuration
OTLP_ENDPOINT = "http://localhost:4317"
SERVICE_NAME = "demo-service"
PARALLEL_SESSIONS = 5
REQUESTS_PER_SESSION = 10
DELAY_BETWEEN_REQUESTS = 0.5
BATCH_PAUSE_SECONDS = 2

# Sample endpoints and status codes
ENDPOINTS = [
    "/api/users",
    "/api/products",
    "/api/orders",
    "/api/checkout",
    "/health",
    "/metrics",
]

STATUS_CODES = ["OK", "ERROR", "UNSET"]
STATUS_WEIGHTS = [0.85, 0.10, 0.05]  # 85% OK, 10% ERROR, 5% UNSET

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]

def setup_telemetry():
    """Set up OpenTelemetry providers."""

    # Create resource with service info
    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: SERVICE_NAME,
        ResourceAttributes.SERVICE_VERSION: "1.0.0",
    })

    # Set up tracing with HTTP exporter
    trace_provider = TracerProvider(resource=resource)
    otlp_trace_exporter = OTLPSpanExporter(
        endpoint=f"{OTLP_ENDPOINT}/v1/traces",
    )
    trace_provider.add_span_processor(BatchSpanProcessor(otlp_trace_exporter))
    trace.set_tracer_provider(trace_provider)

    log_provider = LoggerProvider(resource=resource)
    otlp_log_exporter = OTLPLogExporter(
        endpoint=f"{OTLP_ENDPOINT}/v1/logs",
    )
    log_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
    set_logger_provider(log_provider)

    handler = LoggingHandler(level=logging.NOTSET, logger_provider=log_provider)
    logger = logging.getLogger("telemetry")
    logger.setLevel(logging.INFO)
    logger.addFilter(NullAttributeFilter())
    logger.addHandler(handler)

    return trace.get_tracer(__name__), logger

class NullAttributeFilter(logging.Filter):
    """Normalize log record attributes to avoid NoneType export errors."""

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "taskName", None) is None:
            record.taskName = ""
        return True

def generate_session_telemetry(tracer, logger, session_id, session_label):
    """Generate telemetry for a single session."""

    print(f"\n{'='*60}")
    print(f"Session {session_label}: {session_id}")
    print(f"{'='*60}")

    for req_num in range(REQUESTS_PER_SESSION):
        endpoint = random.choice(ENDPOINTS)
        method = random.choice(HTTP_METHODS)
        status_code = random.choices(STATUS_CODES, STATUS_WEIGHTS)[0]

        # Generate span with session_id
        with tracer.start_as_current_span(f"{method} {endpoint}") as span:
            # Add sp.session.id attribute (REQUIRED)
            span.set_attribute("sp.session.id", session_id)

            # Add HTTP attributes
            span.set_attribute("http.method", method)
            span.set_attribute("http.target", endpoint)
            span.set_attribute("http.request.path", endpoint)
            span.set_attribute("http.status_code", 200 if status_code == "OK" else 500)
            span.set_attribute("http.route", endpoint)

            # Add custom attributes
            span.set_attribute("user.id", f"user-{random.randint(1, 100)}")
            span.set_attribute("request.id", str(uuid.uuid4()))
            span.set_attribute("message_type", "http_request")

            # Set span status
            if status_code == "ERROR":
                span.set_status(Status(StatusCode.ERROR, "Internal server error"))
                logger.error(
                    "request failed",
                    extra={
                        "session_id": session_id,
                        "http.method": method,
                        "http.route": endpoint,
                        "http.status_code": 500,
                    },
                )
            elif status_code == "OK":
                span.set_status(Status(StatusCode.OK))

            # Simulate processing time
            duration_ms = random.randint(10, 500)
            time.sleep(duration_ms / 1000.0)
            span.set_attribute("duration_ms", duration_ms)

            print(f"  [{req_num + 1:2d}/{REQUESTS_PER_SESSION}] {method:6s} {endpoint:20s} "
                  f"status={status_code:5s} duration={duration_ms:3d}ms")

        time.sleep(DELAY_BETWEEN_REQUESTS)

def main():
    """Main function to generate telemetry."""

    print(f"\n🚀 Starting Telemetry Generator")
    print(f"   OTLP Endpoint: {OTLP_ENDPOINT}")
    print(f"   Service: {SERVICE_NAME}")
    print(f"   Sessions (parallel): {PARALLEL_SESSIONS}")
    print(f"   Requests per session: {REQUESTS_PER_SESSION}")
    print(f"   Mode: continuous (Ctrl+C to stop)")

    tracer, logger = setup_telemetry()

    session_counter = 0
    try:
        while True:
            with ThreadPoolExecutor(max_workers=PARALLEL_SESSIONS) as executor:
                futures = []
                for _ in range(PARALLEL_SESSIONS):
                    session_counter += 1
                    session_id = f"session-{uuid.uuid4()}"
                    session_label = f"{session_counter}"
                    futures.append(
                        executor.submit(
                            generate_session_telemetry,
                            tracer,
                            logger,
                            session_id,
                            session_label,
                        )
                    )
                for future in as_completed(futures):
                    future.result()
            print(f"\n⏳ Waiting before next batch...")
            time.sleep(BATCH_PAUSE_SECONDS)
    except KeyboardInterrupt:
        print(f"\n⏳ Flushing telemetry data...")
        time.sleep(5)
        print(f"\n✅ Telemetry generation stopped.")

if __name__ == "__main__":
    main()
