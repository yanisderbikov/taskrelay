package io.github.yanisderbikov.taskrelay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.time.Clock;

@AutoConfiguration(after = {
        DataSourceAutoConfiguration.class,
        FlywayAutoConfiguration.class,
        LiquibaseAutoConfiguration.class,
        JacksonAutoConfiguration.class,
        GsonAutoConfiguration.class})
@ConditionalOnClass(DataSource.class)
@ConditionalOnSingleCandidate(DataSource.class)
@ConditionalOnProperty(prefix = "taskrelay", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(TaskRelayProperties.class)
public class TaskRelayAutoConfiguration {

    @Bean
    @DependsOnDatabaseInitialization
    TaskRelaySchema taskRelaySchema(DataSource dataSource, TaskRelayProperties properties) {
        TaskRelaySchema schema = new TaskRelaySchema(dataSource, new TaskTable(properties.getTableName()),
                properties.getSchema().isAutoCreate());
        schema.ensure();
        return schema;
    }

    @Bean
    TaskStore taskRelayStore(DataSource dataSource, TaskRelayProperties properties, TaskRelaySchema schema) {
        return new TaskStore(dataSource, new TaskTable(properties.getTableName()));
    }

    @Bean
    TaskRegistry taskRelayRegistry(ObjectProvider<TaskHandler<?>> handlers) {
        return new TaskRegistry(handlers);
    }

    @Bean
    @ConditionalOnMissingBean(TaskQueue.class)
    TaskQueue taskQueue(TaskStore store, TaskRegistry registry, TaskPayloadCodec codec, ObjectProvider<Clock> clock) {
        return new JdbcTaskQueue(store, registry, codec, clock.getIfUnique(Clock::systemUTC));
    }

    @Bean
    @ConditionalOnProperty(prefix = "taskrelay.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
    TaskRelayWorkers taskRelayWorkers(TaskStore store, TaskRegistry registry, TaskPayloadCodec codec,
                                      TaskRelayProperties properties, ObjectProvider<Clock> clock) {
        return new TaskRelayWorkers(registry, store, codec, properties, clock.getIfUnique(Clock::systemUTC));
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ObjectMapper.class)
    @ConditionalOnProperty(prefix = "taskrelay", name = "payload-format", havingValue = "jackson", matchIfMissing = true)
    static class JacksonCodecConfiguration {

        @Bean
        @ConditionalOnMissingBean(TaskPayloadCodec.class)
        TaskPayloadCodec taskRelayPayloadCodec(ObjectProvider<ObjectMapper> objectMapper) {
            return new JacksonTaskPayloadCodec(objectMapper.getIfAvailable(() -> new ObjectMapper().findAndRegisterModules()));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "com.google.gson.Gson")
    @ConditionalOnProperty(prefix = "taskrelay", name = "payload-format", havingValue = "gson")
    static class GsonCodecConfiguration {

        @Bean
        @ConditionalOnMissingBean(TaskPayloadCodec.class)
        TaskPayloadCodec taskRelayPayloadCodec() {
            return new GsonTaskPayloadCodec();
        }
    }
}
