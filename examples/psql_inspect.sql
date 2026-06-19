-- ============================================================================
-- Inspecting airnest's PostgreSQL state with psql.
-- ============================================================================
--
-- Run with:
--   psql 'postgres://airnest:airnest@localhost:5432/airnest' \
--        -f examples/psql_inspect.sql
--
-- What this file shows:
--   1.  What tables airnest created
--   2.  The on-disk schema for a #[persistent] type
--   3.  How to read rows using only indexed columns
--   4.  How to format the UUIDv7 id for human inspection
--   5.  How to peek at the raw bitcode blob (for debugging / migrations)
--   6.  The actual indexes airnest created
--
-- Remember: only fields declared via #[persistent(index(...))] become real
-- SQL columns. Other fields live inside the bitcode-encoded `v` blob and can
-- only be read from Rust.
-- ============================================================================

-- Helper: format UUIDv7 bytes as a canonical hyphenated UUID string.
-- BYTEA holds 16 bytes; UUIDs are 16 bytes; we just hex-encode and insert
-- hyphens at the standard positions.
CREATE OR REPLACE FUNCTION format_uuid(b bytea) RETURNS text AS $$
    SELECT lower(
        SUBSTRING(h FROM 1  FOR 8) || '-' ||
        SUBSTRING(h FROM 9  FOR 4) || '-' ||
        SUBSTRING(h FROM 13 FOR 4) || '-' ||
        SUBSTRING(h FROM 17 FOR 4) || '-' ||
        SUBSTRING(h FROM 21 FOR 12)
    )
    FROM (SELECT encode(b, 'hex') AS h) AS t
$$ LANGUAGE SQL IMMUTABLE;

\echo
\echo '=== 1. Tables airnest created ==='
\echo
\dt

\echo
\echo '=== 2. Schema of DemoTask ==='
\echo
-- id      : UUIDv7, 16 bytes, primary key
-- v       : bitcode-encoded blob of the whole struct
-- saved_at: epoch seconds at write time
-- status  : indexed column (declared via #[persistent(index(status, priority))])
-- priority: indexed column
\d "DemoTask"

\echo
\echo '=== 3. Rows, indexed columns only ==='
\echo
-- These columns are real SQL — queryable, joinable, aggregatable.
SELECT
    format_uuid(id)              AS id,
    status,
    priority,
    to_timestamp(saved_at)       AS saved_at
FROM "DemoTask"
ORDER BY saved_at;

\echo
\echo '=== 4. Querying by indexed column ==='
\echo
-- Same filter Rust expresses as find::<DemoTask>().eq("status", "pending").
SELECT
    format_uuid(id)              AS id,
    status,
    priority
FROM "DemoTask"
WHERE status = 'pending'
ORDER BY priority ASC;

\echo
\echo '=== 5. IN filter (multi-value) ==='
\echo
-- Corresponds to find::<DemoTask>().in_("status", &["pending", "running"]).
SELECT
    format_uuid(id)              AS id,
    status,
    priority
FROM "DemoTask"
WHERE status IN ('pending', 'running')
ORDER BY status, priority;

\echo
\echo '=== 6. Aggregates ==='
\echo
-- count_grouped_by in Rust == GROUP BY in SQL.
SELECT status, COUNT(*) AS n
FROM "DemoTask"
GROUP BY status
ORDER BY status;

\echo
\echo '=== 7. The raw bitcode blob ==='
\echo
-- The whole struct is bitcode-encoded into v. Read this only when debugging
-- or writing migrations; normal queries should use the indexed columns.
SELECT
    format_uuid(id)              AS id,
    octet_length(v)              AS v_bytes,
    encode(v, 'hex')             AS v_hex
FROM "DemoTask"
ORDER BY saved_at;

\echo
\echo '=== 8. UUIDv7: time-ordered ids ==='
\echo
-- airnest uses UUIDv7, so ids sort chronologically without an extra index.
-- The first 48 bits are a millisecond timestamp.
SELECT
    format_uuid(id)              AS id,
    saved_at,
    to_timestamp(saved_at)       AS saved_at_human
FROM "DemoTask"
ORDER BY id;

\echo
\echo '=== 9. Indexes airnest created ==='
\echo
\di

\echo
\echo '=== 10. Inspecting DemoUser ==='
\echo
SELECT format_uuid(id) AS id, role FROM "DemoUser" ORDER BY role, format_uuid(id);