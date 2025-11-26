package ai.softprobe.otel.repository;

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.resource.v1.Resource;

import java.util.List;

public interface MetricRepository {
    void storeMetrics(List<Metric> metrics, Resource resource);
}

