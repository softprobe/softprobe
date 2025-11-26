package ai.softprobe.otel.repository;

import ai.softprobe.otel.model.RepositoryMetaInfo;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;

import java.util.List;

public interface SpanRepository {
    Span findMatchingSpan(Span incomingSpan);
    void storeSpans(List<Span> spans, Resource resource, RepositoryMetaInfo metaInfo);
}

