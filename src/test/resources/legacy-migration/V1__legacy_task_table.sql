CREATE TABLE task (
    id         UUID PRIMARY KEY,
    type       VARCHAR(255) NOT NULL,
    payload    TEXT,
    status     VARCHAR(255) NOT NULL,
    comment    TEXT,
    created_at TIMESTAMP(6) WITH TIME ZONE
);
