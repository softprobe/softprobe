package ai.softprobe.otel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties(prefix = "storage")
@Data
public class StorageProperties {
    private String provider = "gcs"; // "gcs" or "s3"
    private String bucketName = "softprobe-otel-data";
    
    public String getStorageUri(String objectName) {
        String scheme = "gcs".equals(provider) ? "gs" : "s3";
        return scheme + "://" + bucketName + "/" + objectName;
    }
    
    public String getStorageUri(String objectName, String dynamicBucketName) {
        String scheme = "gcs".equals(provider) ? "gs" : "s3";
        String targetBucket = (dynamicBucketName != null && !dynamicBucketName.trim().isEmpty()) 
            ? dynamicBucketName : bucketName;
        return scheme + "://" + targetBucket + "/" + objectName;
    }
    
    public boolean isGcs() {
        return "gcs".equals(provider);
    }
    
    public boolean isS3() {
        return "s3".equals(provider);
    }
}

