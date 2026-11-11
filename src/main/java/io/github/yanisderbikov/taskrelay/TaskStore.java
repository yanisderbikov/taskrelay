package io.github.yanisderbikov.taskrelay;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

final class TaskStore {

    private static final String COLUMNS =
            "id, type, status, payload, dedupe_key, attempts, comment, created_at, next_attempt_at, updated_at";
    private static final int CLEANUP_CHUNK = 1000;

    private final NamedParameterJdbcTemplate jdbc;
    private final String table;
    private final RowMapper<TaskRecord> mapper = TaskStore::map;

    TaskStore(DataSource dataSource, TaskTable table) {
        this.jdbc = new NamedParameterJdbcTemplate(dataSource);
        this.table = table.name();
    }

    boolean insert(UUID id, String type, String payload, String dedupeKey, Instant runAt, Instant now) {
        String sql = "INSERT INTO " + table
                + " (id, type, payload, status, dedupe_key, attempts, next_attempt_at, created_at, updated_at)"
                + " VALUES (:id, :type, :payload, 'NEW', :dedupeKey, 0, :runAt, :now, :now)"
                + (dedupeKey == null ? "" : " ON CONFLICT (type, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("type", type)
                .addValue("payload", payload, Types.VARCHAR)
                .addValue("dedupeKey", dedupeKey, Types.VARCHAR)
                .addValue("runAt", time(runAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE);
        return jdbc.update(sql, params) == 1;
    }

    List<TaskRecord> claim(String type, int limit, Instant now, Instant lockedUntil, Instant createdBefore) {
        String sql = "UPDATE " + table + " SET status = 'RUNNING', attempts = attempts + 1,"
                + " locked_until = :lockedUntil, updated_at = :now"
                + " WHERE id IN (SELECT id FROM " + table
                + " WHERE type = :type"
                + " AND ((status = 'NEW' AND (next_attempt_at IS NULL OR next_attempt_at <= :now))"
                + " OR (status = 'RUNNING' AND locked_until < :now))"
                + (createdBefore == null ? "" : " AND created_at <= :createdBefore")
                + " ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED)"
                + " RETURNING " + COLUMNS;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("type", type)
                .addValue("limit", limit)
                .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("lockedUntil", time(lockedUntil), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("createdBefore", time(createdBefore), Types.TIMESTAMP_WITH_TIMEZONE);
        return jdbc.query(sql, params, mapper).stream()
                .sorted(Comparator.comparing(TaskRecord::createdAt, Comparator.nullsFirst(Comparator.naturalOrder())))
                .toList();
    }

    void markDone(UUID id, Instant now) {
        jdbc.update("UPDATE " + table + " SET status = 'DONE', locked_until = NULL, updated_at = :now WHERE id = :id",
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE));
    }

    void reschedule(UUID id, Instant nextAttemptAt, String comment, Instant now) {
        jdbc.update("UPDATE " + table + " SET status = 'NEW', next_attempt_at = :next, comment = :comment,"
                        + " locked_until = NULL, updated_at = :now WHERE id = :id",
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("next", time(nextAttemptAt), Types.TIMESTAMP_WITH_TIMEZONE)
                        .addValue("comment", comment, Types.VARCHAR)
                        .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE));
    }

    void markFailed(UUID id, String comment, Instant now) {
        jdbc.update("UPDATE " + table + " SET status = 'FAILED', comment = :comment,"
                        + " locked_until = NULL, updated_at = :now WHERE id = :id",
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("comment", comment, Types.VARCHAR)
                        .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE));
    }

    void release(Collection<UUID> ids, Instant now) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.update("UPDATE " + table + " SET status = 'NEW', attempts = GREATEST(attempts - 1, 0),"
                        + " locked_until = NULL, updated_at = :now WHERE id IN (:ids) AND status = 'RUNNING'",
                new MapSqlParameterSource()
                        .addValue("ids", ids)
                        .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE));
    }

    boolean retry(UUID id, Instant now) {
        return jdbc.update("UPDATE " + table + " SET status = 'NEW', attempts = 0, next_attempt_at = NULL,"
                        + " locked_until = NULL, updated_at = :now WHERE id = :id AND status = 'FAILED'",
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("now", time(now), Types.TIMESTAMP_WITH_TIMEZONE)) == 1;
    }

    Optional<TaskRecord> find(UUID id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM " + table + " WHERE id = :id",
                new MapSqlParameterSource("id", id), mapper).stream().findFirst();
    }

    List<TaskRecord> findByType(String type, Integer limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + table + " WHERE type = :type ORDER BY created_at DESC"
                + (limit == null ? "" : " LIMIT :limit");
        MapSqlParameterSource params = new MapSqlParameterSource("type", type);
        if (limit != null) {
            params.addValue("limit", limit);
        }
        return jdbc.query(sql, params, mapper);
    }

    int deleteDoneBefore(Instant cutoff) {
        String sql = "DELETE FROM " + table + " WHERE id IN (SELECT id FROM " + table
                + " WHERE status = 'DONE' AND COALESCE(updated_at, created_at) < :cutoff LIMIT " + CLEANUP_CHUNK + ")";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("cutoff", time(cutoff), Types.TIMESTAMP_WITH_TIMEZONE);
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.update(sql, params);
            total += deleted;
        } while (deleted == CLEANUP_CHUNK);
        return total;
    }

    private static TaskRecord map(ResultSet rs, int row) throws SQLException {
        return new TaskRecord(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                TaskStatus.valueOf(rs.getString("status")),
                rs.getString("payload"),
                rs.getString("dedupe_key"),
                rs.getInt("attempts"),
                rs.getString("comment"),
                instant(rs, "created_at"),
                instant(rs, "next_attempt_at"),
                instant(rs, "updated_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static OffsetDateTime time(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }
}
