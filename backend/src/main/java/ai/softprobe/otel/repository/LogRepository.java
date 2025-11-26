package ai.softprobe.otel.repository;

import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;

import java.util.List;

public interface LogRepository {
    void storeLogs(List<LogRecord> logRecords, Resource resource);
}

