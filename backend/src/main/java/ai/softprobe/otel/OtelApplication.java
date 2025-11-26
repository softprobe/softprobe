package ai.softprobe.otel;

import org.springframework.boot.SpringApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import ai.softprobe.otel.config.ClickHouseProperties;
import ai.softprobe.otel.config.StorageProperties;

@SpringBootApplication
@EnableConfigurationProperties({ClickHouseProperties.class, StorageProperties.class})
@EnableCaching
public class OtelApplication {
    public static void main(String[] args) {
        SpringApplication.run(OtelApplication.class, args);
    }
}

