package ai.softprobe.otel.repository;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import ai.softprobe.otel.model.TraceQueryFilter;

public interface QueryRepository {
    ExportTraceServiceRequest queryTraces(TraceQueryFilter filter);
}

