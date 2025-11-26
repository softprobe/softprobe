package ai.softprobe.otel.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenAPIConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("OTLP Collector API")
                        .description("OpenTelemetry Protocol (OTLP) Collector API supporting both protobuf and JSON formats for ingesting and querying telemetry data (traces, metrics, and logs)")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Softprobe AI")
                                .url("https://softprobe.ai"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Development server")
                ));
    }
}

