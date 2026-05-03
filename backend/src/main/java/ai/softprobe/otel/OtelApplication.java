package ai.softprobe.otel;

import org.springframework.boot.SpringApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import ai.softprobe.otel.config.ClickHouseProperties;
import ai.softprobe.otel.config.StorageProperties;
import ai.softprobe.otel.config.InjectionProperties;

@SpringBootApplication(exclude = {
    com.google.cloud.spring.autoconfigure.storage.GcpStorageAutoConfiguration.class
})
@EnableConfigurationProperties({ClickHouseProperties.class, StorageProperties.class, InjectionProperties.class})
@EnableCaching
public class OtelApplication {
    public static void main(String[] args) {
        SpringApplication.run(OtelApplication.class, args);
    }
}

