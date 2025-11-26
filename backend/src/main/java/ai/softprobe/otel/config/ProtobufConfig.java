package ai.softprobe.otel.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class ProtobufConfig implements WebMvcConfigurer {

    @Bean
    public ProtobufHttpMessageConverter protobufHttpMessageConverter() {
        ProtobufHttpMessageConverter converter = new ProtobufHttpMessageConverter();
        // Ensure protobuf converter can handle the response content type
        converter.setSupportedMediaTypes(List.of(
            MediaType.valueOf("application/x-protobuf"),
            MediaType.valueOf("application/protobuf")
        ));
        return converter;
    }

    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer
            .defaultContentType(MediaType.APPLICATION_JSON)
            .mediaType("protobuf", MediaType.valueOf("application/x-protobuf"))
            .mediaType("pb", MediaType.valueOf("application/x-protobuf"));
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        // Ensure protobuf converter is first in the list for proper handling
        converters.add(0, protobufHttpMessageConverter());
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH")
                .allowedHeaders("*")
                .exposedHeaders("Content-Type", "X-Requested-With", "accept", "Origin", "Access-Control-Request-Method", 
                               "Access-Control-Request-Headers", "Authorization")
                .allowCredentials(true)
                .maxAge(7200);
    }
}

