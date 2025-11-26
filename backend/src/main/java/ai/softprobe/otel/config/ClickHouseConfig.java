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
        props.setProperty("database", properties.getDatabase());
        
        return new ClickHouseDataSource(properties.getUrl(), props);
    }
}

