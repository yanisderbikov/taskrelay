package io.github.yanisderbikov.taskrelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

final class TaskRelaySchema {

    private static final Logger log = LoggerFactory.getLogger(TaskRelaySchema.class);

    private final JdbcTemplate jdbc;
    private final TaskTable table;
    private final boolean autoCreate;

    TaskRelaySchema(DataSource dataSource, TaskTable table, boolean autoCreate) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.table = table;
        this.autoCreate = autoCreate;
    }

    void ensure() {
        if (!autoCreate) {
            return;
        }
        String name = table.name();
        Boolean exists = jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, name);
        if (!Boolean.TRUE.equals(exists)) {
            jdbc.execute("CREATE TABLE IF NOT EXISTS " + name + " ("
                    + "id UUID PRIMARY KEY, "
                    + "type VARCHAR(255) NOT NULL, "
                    + "payload TEXT, "
                    + "status VARCHAR(32) NOT NULL, "
                    + "dedupe_key VARCHAR(255), "
                    + "attempts INTEGER NOT NULL DEFAULT 0, "
                    + "comment TEXT, "
                    + "next_attempt_at TIMESTAMPTZ, "
                    + "locked_until TIMESTAMPTZ, "
                    + "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    + "updated_at TIMESTAMPTZ)");
            jdbc.execute("CREATE INDEX IF NOT EXISTS " + table.indexName("due_idx")
                    + " ON " + name + " (type, status, created_at)");
            log.info("TaskRelay created table {}", name);
        }
        jdbc.execute("ALTER TABLE " + name
                + " ADD COLUMN IF NOT EXISTS dedupe_key VARCHAR(255),"
                + " ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,"
                + " ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,"
                + " ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,"
                + " ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ");
        jdbc.execute("CREATE UNIQUE INDEX IF NOT EXISTS " + table.indexName("dedupe_key_uq")
                + " ON " + name + " (type, dedupe_key) WHERE dedupe_key IS NOT NULL");
    }
}
