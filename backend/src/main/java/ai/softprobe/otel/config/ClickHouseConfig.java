package ai.softprobe.otel.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

@Configuration
public class ClickHouseConfig {

    @Autowired
    private ClickHouseProperties properties;

    @Bean
    public DataSource clickHouseDataSource() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", properties.getUsername());
        if (properties.getPassword() != null && !properties.getPassword().isEmpty()) {
            props.setProperty("password", properties.getPassword());
        }
        // Connect to 'default' database initially, we'll create the target database later
        props.setProperty("database", "default");
        // Disable compression to avoid LZ4 dependency issues
        props.setProperty("compress", "0");
        
        // Always use 'default' database in URL for initial connection
        // Extract host and port from the original URL
        String url = properties.getUrl();
        // Replace the database part with 'default'
        // URL format: jdbc:clickhouse://host:port/database
        url = url.replaceFirst("/([^/]+)$", "/default");
        
        return new ClickHouseDataSource(url, props);
    }
}

