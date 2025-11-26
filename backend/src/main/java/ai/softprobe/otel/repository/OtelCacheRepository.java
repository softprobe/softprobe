package ai.softprobe.otel.repository;

import ai.softprobe.otel.model.RepositoryMetaInfo;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import ai.softprobe.otel.config.InjectionProperties;
import ai.softprobe.otel.config.StorageProperties;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.BucketInfo;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

@Slf4j
@Repository
public class OtelCacheRepository implements SpanRepository {

    private final RedisTemplate<String, String> redisTemplate;
    private final ResourceLoader resourceLoader;
    private final StorageProperties storageProperties;
    private final InjectionProperties injectionProperties;
    private final Storage storage;
    private final com.google.gson.Gson gson = new com.google.gson.GsonBuilder()
            .registerTypeAdapter(Instant.class, new com.google.gson.JsonSerializer<Instant>() {
                @Override
                public com.google.gson.JsonElement serialize(Instant src, java.lang.reflect.Type typeOfSrc, com.google.gson.JsonSerializationContext context) {
                    return new com.google.gson.JsonPrimitive(src.toString());
                }
            })
            .registerTypeAdapter(Instant.class, new com.google.gson.JsonDeserializer<Instant>() {
                @Override
                public Instant deserialize(com.google.gson.JsonElement json, java.lang.reflect.Type typeOfT, com.google.gson.JsonDeserializationContext context) throws com.google.gson.JsonParseException {
                    return Instant.parse(json.getAsString());
                }
            })
            .create();


    @Autowired
    public OtelCacheRepository(RedisTemplate<String, String> redisTemplate,
                               ResourceLoader resourceLoader,
                               StorageProperties storageProperties,
                               InjectionProperties injectionProperties,
                               Storage storage) {
        this.redisTemplate = redisTemplate;
        this.resourceLoader = resourceLoader;
        this.storageProperties = storageProperties;
        this.injectionProperties = injectionProperties;
        this.storage = storage;
    }

    @Override
    public void storeSpans(List<Span> spans, io.opentelemetry.proto.resource.v1.Resource resource, RepositoryMetaInfo metaInfo) {
        String bucketName = (metaInfo != null && metaInfo.getBucketName() != null) 
            ? metaInfo.getBucketName() 
            : storageProperties.getBucketName();
        storeSpans(spans, resource, bucketName);
    }

    public void storeSpans(List<Span> spans, io.opentelemetry.proto.resource.v1.Resource resource, String bucketName) {

        for (Span span : spans) {
            // Try body hash first (for POST, PUT, etc. requests)
            String requestHash = extractAndHashRequestBody(span);

            // For requests without body (e.g. GET), use content-based hash
            if (requestHash == null) {
                requestHash = extractAndHashRequestContent(span);
            }

            if (requestHash != null) {
                Map<String, Object> attributes = transformKeyValueList(span.getAttributesList());
                Map<String, Object> responseData = new HashMap<>();
                responseData.put("requestHash", extractAndHashRequestBody(span));
                responseData.put("timestamp", java.time.Instant.now().toString());
                responseData.put("attributes", attributes);

                // 如果有 bucketName，添加到响应数据中
                if (bucketName != null && !bucketName.trim().isEmpty()) {
                    responseData.put("bucket_name", bucketName);
                }

                storeInStorageAndIndex(requestHash, responseData, bucketName);
            }
        }

        if (bucketName != null && !bucketName.trim().isEmpty()) {
            log.info("Stored {} spans in cache with bucketName: {}", spans.size(), bucketName);
        } else {
            log.debug("Stored {} spans in cache with default logic", spans.size());
        }
    }


    @Override
    public Span findMatchingSpan(Span incomingSpan) {
        // Try body hash first (for POST, PUT, etc. requests)
        String requestHash = extractAndHashRequestBody(incomingSpan);

        // For requests without body (e.g. GET), use content-based hash
        if (requestHash == null) {
            requestHash = extractAndHashRequestContent(incomingSpan);
        }

        String attributesJson = lookupInCache(requestHash);

        if (attributesJson != null) {
            return rebuildSpanWithCachedAttributes(incomingSpan, attributesJson);
        }
        return null;
    }

    private String extractAndHashRequestBody(Span span) {
        try {
            for (io.opentelemetry.proto.common.v1.KeyValue attribute : span.getAttributesList()) {
                if ("http.request.body".equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                    String requestBody = attribute.getValue().getStringValue();
                    if (requestBody != null && !requestBody.isEmpty()) {
                        CRC32 crc32 = new CRC32();
                        crc32.update(requestBody.getBytes(StandardCharsets.UTF_8));
                        return String.valueOf(crc32.getValue());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to extract request body hash for span {}: {}", span.getSpanId(), e.getMessage());
        }
        return null;
    }

    /**
     * Generates a content hash based on HTTP method, URL path and query parameters for GET requests
     * This is used as a fallback when there's no request body (e.g. GET requests)
     *
     * @param span the OpenTelemetry span to extract request information from
     * @return the hash of the request content, or null if not enough information
     */
    private String extractAndHashRequestContent(Span span) {
        try {
            String method = null;
            String urlPath = null;

            // Extract HTTP method, URL path and query string from span attributes
            for (io.opentelemetry.proto.common.v1.KeyValue attribute : span.getAttributesList()) {
                switch (attribute.getKey()) {
                    case "http.request.header.:method":
                        if (attribute.getValue().hasStringValue()) {
                            method = attribute.getValue().getStringValue();
                        }
                        break;
                    case "http.request.header.:path":
                        if (attribute.getValue().hasStringValue()) {
                            urlPath = attribute.getValue().getStringValue();
                        }
                        break;
                }
            }

            // Only proceed if we have at least the HTTP method
            if (method != null) {
                StringBuilder content = new StringBuilder();
                content.append("method:").append(method);

                if (urlPath != null) {
                    content.append("|path:").append(urlPath);
                }

                // Create a hash of the content for efficient lookups
                CRC32 crc32 = new CRC32();
                crc32.update(content.toString().getBytes(StandardCharsets.UTF_8));
                return String.valueOf(crc32.getValue());
            }
        } catch (Exception e) {
            log.warn("Failed to extract request content hash for span {}: {}", span.getSpanId(), e.getMessage());
        }
        return null;
    }

    private String lookupInCache(String requestHash) {
        if (requestHash == null || requestHash.isEmpty()) {
            return null;
        }

        try {
            // Check Redis index first
            String redisKey = "req:" + requestHash;
            String s3Key = redisTemplate.opsForValue().get(redisKey);

            if (s3Key != null) {
                // Fetch from storage
                return fetchFromStorage(s3Key);
            }
        } catch (Exception e) {
            log.warn("Failed to lookup in cache for hash {}: {}", requestHash, e.getMessage());
        }

        return null;
    }

    private String fetchFromStorage(String storagePath) {
        try {
            org.springframework.core.io.Resource storageResource = resourceLoader.getResource(storagePath);
            if (storageResource.exists()) {
                try (InputStream inputStream = storageResource.getInputStream()) {
                    return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
            return null;
        } catch (Exception e) {
            log.error("Failed to fetch from storage path {}: {}", storagePath, e.getMessage());
            return null;
        }
    }

    private Span rebuildSpanWithCachedAttributes(Span original, String attributesJson) {
        try {
            Map<String, Object> responseData = gson.fromJson(attributesJson, Map.class);

            if (responseData != null && responseData.containsKey("attributes")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> attributesMap = (Map<String, Object>) responseData.get("attributes");
                List<io.opentelemetry.proto.common.v1.KeyValue> attributes = convertMapToKeyValueList(attributesMap);

                return original.toBuilder()
                        .clearAttributes()
                        .addAllAttributes(attributes)
                        .build();
            }
        } catch (Exception e) {
            log.error("Failed to rebuild span with cached attributes: {}", attributesJson, e);
        }
        return original;
    }

    private List<io.opentelemetry.proto.common.v1.KeyValue> convertMapToKeyValueList(Map<String, Object> attributesMap) {
        List<io.opentelemetry.proto.common.v1.KeyValue> attributes = new ArrayList<>();

        for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
            io.opentelemetry.proto.common.v1.AnyValue value = convertObjectToAnyValue(entry.getValue());

            io.opentelemetry.proto.common.v1.KeyValue attribute = io.opentelemetry.proto.common.v1.KeyValue.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(value)
                    .build();
            attributes.add(attribute);
        }

        return attributes;
    }

    private io.opentelemetry.proto.common.v1.AnyValue convertObjectToAnyValue(Object val) {
        if (val instanceof String) {
            return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                    .setStringValue((String) val)
                    .build();
        } else if (val instanceof Boolean) {
            return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                    .setBoolValue((Boolean) val)
                    .build();
        } else if (val instanceof Number) {
            if (val instanceof Integer || val instanceof Long) {
                return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                        .setIntValue(((Number) val).longValue())
                        .build();
            } else {
                return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                        .setDoubleValue(((Number) val).doubleValue())
                        .build();
            }
        } else {
            return io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                    .setStringValue(val.toString())
                    .build();
        }
    }

    private String transformSpanToAttributesJson(Map<String, Object> responseData, io.opentelemetry.proto.resource.v1.Resource resource) {
        return gson.toJson(responseData);
    }

    private Map<String, Object> transformKeyValueList(List<io.opentelemetry.proto.common.v1.KeyValue> keyValueList) {
        Map<String, Object> result = new HashMap<>();
        for (io.opentelemetry.proto.common.v1.KeyValue kv : keyValueList) {
            // Keep original key format for proper reconstruction
            String key = kv.getKey();
            result.put(key, transformAnyValue(kv.getValue()));
        }
        return result;
    }

    private Object transformAnyValue(io.opentelemetry.proto.common.v1.AnyValue anyValue) {
        if (anyValue.hasStringValue()) {
            return anyValue.getStringValue();
        } else if (anyValue.hasIntValue()) {
            return anyValue.getIntValue();
        } else if (anyValue.hasDoubleValue()) {
            return anyValue.getDoubleValue();
        } else if (anyValue.hasBoolValue()) {
            return anyValue.getBoolValue();
        } else if (anyValue.hasBytesValue()) {
            return bytesToHex(anyValue.getBytesValue().toByteArray());
        } else if (anyValue.hasArrayValue()) {
            List<Object> array = new ArrayList<>();
            anyValue.getArrayValue().getValuesList().forEach(value -> array.add(transformAnyValue(value)));
            return array;
        } else if (anyValue.hasKvlistValue()) {
            return transformKeyValueList(anyValue.getKvlistValue().getValuesList());
        }
        return null;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private void storeInStorageAndIndex(String hash, Map<String, Object> responseData, String bucketName) {
        try {
            log.info("=== Starting storeInStorageAndIndex with bucketName: {} ===", bucketName);
            log.info("DEBUG: storageProperties.isGcs() = {}", storageProperties.isGcs());
            
            if (storageProperties.isGcs()) {
                log.info("DEBUG: About to call ensureBucketExists for bucket: {}", bucketName);
                // 确保bucket存在
                ensureBucketExists(bucketName);
                log.info("=== After ensureBucketExists call ===");
            } else {
                log.info("DEBUG: Storage provider is not GCS, skipping bucket creation");
            }
            
            String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String objectName = genObjectName(date, hash, responseData);
            log.info("Storing span in storage: {}", objectName);
            // Store using Spring Resource abstraction (works with both S3 and GCS)
            String storagePath = storageProperties.getStorageUri(objectName, bucketName);
            org.springframework.core.io.Resource storageResource = resourceLoader.getResource(storagePath);

            if (storageResource instanceof WritableResource writableResource) {
                try (OutputStream outputStream = writableResource.getOutputStream()) {
                    outputStream.write(gson.toJson(responseData).getBytes(StandardCharsets.UTF_8));
                }
            } else {
                throw new IllegalStateException("Storage resource is not writable: " + storagePath);
            }

            // Index in Redis
            String redisKey = "req:" + hash;
            redisTemplate.opsForValue().set(redisKey, storagePath, injectionProperties.getCacheTtlSeconds(), TimeUnit.SECONDS);

            // Add to date index for cleanup
            String dateIndexKey = "idx:date:" + date;
            redisTemplate.opsForSet().add(dateIndexKey, hash);
            redisTemplate.expire(dateIndexKey, injectionProperties.getCacheTtlSeconds(), TimeUnit.SECONDS);

            log.debug("Stored and indexed span data for hash {} in storage object {} (provider: {})",
                    hash, objectName, storageProperties.getProvider());

        } catch (Exception e) {
            log.error("Failed to store and index span data for hash {}: {}", hash, e.getMessage(), e);
        }
    }
    
    /**
     * 确保bucket存在，如果不存在则创建
     */
    private void ensureBucketExists(String bucketName) {
        String finalBucketName = resolveBucketName(bucketName);
        
        if (!shouldCheckBucket()) {
            return;
        }
        
        log.info("Ensuring bucket exists: {}", finalBucketName);
        
        try {
            if (bucketExists(finalBucketName)) {
                log.debug("Bucket {} already exists", finalBucketName);
                return;
            }
            
            createBucket(finalBucketName);
            log.info("Successfully ensured bucket: {}", finalBucketName);
            
        } catch (Exception e) {
            log.warn("Failed to ensure bucket {}: {}. Proceeding anyway...", 
                    finalBucketName, e.getMessage());
        }
    }
    
    private String resolveBucketName(String bucketName) {
        return (bucketName == null || bucketName.trim().isEmpty()) 
                ? storageProperties.getBucketName() 
                : bucketName;
    }
    
    private boolean shouldCheckBucket() {
        if (!storageProperties.isGcs()) {
            log.debug("Skipping bucket check - not using GCS provider (current: {})", 
                    storageProperties.getProvider());
            return false;
        }
        return true;
    }
    
    private boolean bucketExists(String bucketName) throws StorageException {
        try {
            com.google.cloud.storage.Bucket bucket = storage.get(bucketName);
            return bucket != null;
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                return false; // Bucket doesn't exist
            }
            if (e.getCode() == 409) {
                return true; // Bucket exists but we got a conflict
            }
            throw e; // Re-throw other storage exceptions
        }
    }
    
    private void createBucket(String bucketName) throws StorageException {
        try {
            BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName).build();
            storage.create(bucketInfo);
            log.info("Successfully created bucket: {}", bucketName);
        } catch (StorageException e) {
            if (e.getCode() == 409) {
                log.debug("Bucket {} was created by another process", bucketName);
                return; // This is fine, bucket exists now
            }
            throw e; // Re-throw other storage exceptions
        }
    }

    private String genObjectName(String date, String hash, Map<String, Object> responseData) {
        // Generate object name with date partitioning
        String uuid = UUID.randomUUID().toString();
        Map<String, Object> attributes = (Map<String, Object>) responseData.get("attributes");
        String serverName = (String) attributes.get("sp.service.name");
        String trafficDirection = (String) attributes.get("sp.traffic.direction");
        if (Strings.isBlank(serverName) || Strings.isBlank(trafficDirection)) {
            return genObjectNameDefault(date, hash);
        }
        if (trafficDirection.equals("inbound")) {
            String path = getRequestPath(attributes);
            return String.format("%s/%s/inbound/%s/%s/%s.json", date, serverName, path, hash, uuid);
        }
        if (trafficDirection.equals("outbound")) {
            String host = getRequestHost(attributes);
            String path = getRequestPath(attributes);
            return String.format("%s/%s/outbound/%s/%s/%s/%s.json", date, serverName, host, path, hash, uuid);
        }
        return genObjectNameDefault(date, hash);
    }

    private String genObjectNameDefault(String date, String hash) {
        String uuid = UUID.randomUUID().toString();
        return String.format("%s/%s/%s.json", date, hash, uuid);
    }

    private String getRequestHost(Map<String, Object> responseData) {
        if (responseData.containsKey("http.request.header.:authority")) {
            return (String) responseData.get("http.request.header.:authority");
        }
        if (responseData.containsKey("http.request.header.host")) {
            return (String) responseData.get("http.request.header.host");
        }
        return null;
    }

    private String getRequestPath(Map<String, Object> responseData) {
        if (responseData.containsKey("http.request.header.:path")) {
            String path = (String) responseData.get("http.request.header.:path");
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            return path;
        }
        return null;
    }

}

