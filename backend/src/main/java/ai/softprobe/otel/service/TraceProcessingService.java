package ai.softprobe.otel.service;

import ai.softprobe.otel.model.RepositoryMetaInfo;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import ai.softprobe.otel.repository.SpanRepository;
import ai.softprobe.otel.repository.OtelCacheRepository;
import ai.softprobe.otel.util.OtlpFormatUtils;
import ai.softprobe.otel.config.ClickHouseProperties;
import ai.softprobe.otel.config.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class TraceProcessingService {

    @Autowired
    private SpanRepository spanRepository;

    @Autowired
    private OtelCacheRepository cacheRepository;

    @Autowired
    private ClickHouseProperties clickHouseProperties;

    @Autowired
    private StorageProperties storageProperties;

    /**
     * 处理 trace 数据
     */
    public void processTraces(byte[] body, String contentType) {
        ExportTraceServiceRequest request = OtlpFormatUtils.parseTraceRequest(body, contentType);
        
        // 打印 JSON 格式的 request
        try {
            String jsonRequest = JsonFormat.printer().print(request);
            log.info("Received ExportTraceServiceRequest JSON: {}", jsonRequest);
        } catch (Exception e) {
            log.warn("Failed to convert request to JSON: {}", e.getMessage());
        }
        
        for (var resourceSpans : request.getResourceSpansList()) {
            List<Span> spans = new ArrayList<>();
            var resource = resourceSpans.getResource();
            for (var scopeSpans : resourceSpans.getScopeSpansList()) {
                spans.addAll(scopeSpans.getSpansList());
            }
            
            // 使用固定的数据库和bucket配置
            RepositoryMetaInfo metaInfo = new RepositoryMetaInfo();
            metaInfo.setDatabase(clickHouseProperties.getDatabase());
            metaInfo.setBucketName(storageProperties.getBucketName());
            
            // 存储 spans 到 ClickHouse
            storeSpansInClickHouse(spans, resource, metaInfo);
            // 存储 spans 到缓存
            storeSpansInCache(spans, resource, metaInfo);
            log.info("Ingested {} traces successfully", spans.size());
        }
    }

    /**
     * 存储 spans 到 ClickHouse
     */
    private void storeSpansInClickHouse(List<Span> spans, Resource resource, RepositoryMetaInfo metaInfo) {
        spanRepository.storeSpans(spans, resource, metaInfo);
    }

    /**
     * 存储 spans 到缓存
     */
    private void storeSpansInCache(List<Span> spans, Resource resource, RepositoryMetaInfo metaInfo) {
        cacheRepository.storeSpans(spans, resource, metaInfo);
    }
}

