package io.github.yanisderbikov.taskrelay;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class TaskRelayAutoConfigurationTest {

    private ApplicationContextRunner runner(String table) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        DataSourceTransactionManagerAutoConfiguration.class,
                        TransactionAutoConfiguration.class,
                        JacksonAutoConfiguration.class,
                        TaskRelayAutoConfiguration.class))
                .withPropertyValues(
                        "spring.datasource.url=" + PostgresSupport.POSTGRES.getJdbcUrl(),
                        "spring.datasource.username=" + PostgresSupport.POSTGRES.getUsername(),
                        "spring.datasource.password=" + PostgresSupport.POSTGRES.getPassword(),
                        "spring.flyway.enabled=false",
                        "taskrelay.table-name=" + table,
                        "taskrelay.initial-delay=0s",
                        "taskrelay.poll-interval=100ms");
    }

    @Configuration(proxyBeanMethods = false)
    static class Handlers {
        @Bean
        RecordingHandler welcomeEmail() {
            return new RecordingHandler("WELCOME_EMAIL");
        }
    }

    @Test
    void enqueuedTasksAreProcessedInTheBackground() {
        runner(PostgresSupport.uniqueTable()).withUserConfiguration(Handlers.class).run(context -> {
            TaskQueue queue = context.getBean(TaskQueue.class);
            RecordingHandler handler = context.getBean(RecordingHandler.class);

            UUID id = queue.enqueue(new RecordingHandler.Payload("hi"));

            await().atMost(Duration.ofSeconds(10)).until(() -> queue.find(id).orElseThrow().status() == TaskStatus.DONE);
            assertThat(handler.handled).containsExactly("hi");
        });
    }

    @Test
    void workersCanBeSwitchedOffWhileTheQueueKeepsWorking() {
        runner(PostgresSupport.uniqueTable()).withUserConfiguration(Handlers.class)
                .withPropertyValues("taskrelay.worker.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(TaskRelayWorkers.class);
                    TaskQueue queue = context.getBean(TaskQueue.class);
                    UUID id = queue.enqueue(new RecordingHandler.Payload("hi"));
                    Thread.sleep(300);
                    assertThat(queue.find(id).orElseThrow().status()).isEqualTo(TaskStatus.NEW);
                });
    }

    @Test
    void perTypeSettingsAcceptUpperCaseTypeKeys() {
        runner(PostgresSupport.uniqueTable()).withUserConfiguration(Handlers.class)
                .withPropertyValues("taskrelay.types[WELCOME_EMAIL].enabled=false")
                .run(context -> {
                    TaskQueue queue = context.getBean(TaskQueue.class);
                    UUID id = queue.enqueue(new RecordingHandler.Payload("hi"));
                    Thread.sleep(300);
                    assertThat(queue.find(id).orElseThrow().status()).isEqualTo(TaskStatus.NEW);
                });
    }

    @Test
    void gsonFormatUsesGsonCodec() {
        runner(PostgresSupport.uniqueTable()).withPropertyValues("taskrelay.payload-format=gson")
                .run(context -> assertThat(context.getBean(TaskPayloadCodec.class)).isInstanceOf(GsonTaskPayloadCodec.class));
    }

    @Test
    void duplicateTaskTypesAreRejected() {
        runner(PostgresSupport.uniqueTable())
                .withBean("first", RecordingHandler.class, () -> new RecordingHandler("SAME"))
                .withBean("second", RecordingHandler.class, () -> new RecordingHandler("SAME"))
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void schemaIsUpgradedAfterFlywayCreatedTheLegacyTable() {
        String schema = "s_" + UUID.randomUUID().toString().replace("-", "");
        runner(schema + ".task")
                .withConfiguration(AutoConfigurations.of(FlywayAutoConfiguration.class))
                .withUserConfiguration(Handlers.class)
                .withPropertyValues(
                        "spring.flyway.enabled=true",
                        "spring.flyway.schemas=" + schema,
                        "spring.flyway.locations=classpath:legacy-migration")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    TaskQueue queue = context.getBean(TaskQueue.class);
                    UUID id = queue.enqueue(new RecordingHandler.Payload("after flyway"));
                    await().atMost(Duration.ofSeconds(10)).until(() -> queue.find(id).orElseThrow().status() == TaskStatus.DONE);
                });
    }

    @Test
    void enqueueParticipatesInJpaTransactions() {
        runner(PostgresSupport.uniqueTable())
                .withConfiguration(AutoConfigurations.of(HibernateJpaAutoConfiguration.class))
                .withUserConfiguration(Handlers.class)
                .withPropertyValues("taskrelay.worker.enabled=false")
                .run(context -> {
                    assertThat(context.getBean(PlatformTransactionManager.class).getClass().getSimpleName())
                            .isEqualTo("JpaTransactionManager");
                    TaskQueue queue = context.getBean(TaskQueue.class);
                    TransactionTemplate tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
                    String table = context.getBean(TaskRelayProperties.class).getTableName();
                    JdbcTemplate jdbc = new JdbcTemplate(context.getBean(javax.sql.DataSource.class));

                    assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
                        queue.enqueue(new RecordingHandler.Payload("rolled back"));
                        throw new IllegalStateException("boom");
                    })).isInstanceOf(IllegalStateException.class);

                    assertThat(jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class)).isZero();
                });
    }
}
