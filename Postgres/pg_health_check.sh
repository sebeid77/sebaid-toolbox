#!/usr/bin/env bash
#
# PostgreSQL Health Check Script
#
# Disclaimer:
# This script is provided as a DBA health-check utility and reporting aid.
# It is intended for review, monitoring, and investigation purposes only.
# Findings in this report are heuristic and should not be treated as automatic
# proof of a problem. Always validate recommendations against workload patterns,
# maintenance windows, replication topology, application behavior, and change
# management policy before taking action.
#
# This script does NOT make database changes such as DROP INDEX, VACUUM FULL,
# REINDEX, or configuration changes. It only collects and formats information.
#
# Requirements:
# - bash
# - psql client installed and reachable in PATH
# - Network access to the PostgreSQL server
# - A PostgreSQL login with access to system catalog/statistics views
# - pg_stat_statements extension enabled for query statistics sections
#
# Usage:
#   Interactive mode:
#     ./pg_health_check.sh
#
#   Connection string mode:
#     ./pg_health_check.sh -d "host=dbhost port=5432 dbname=postgres user=monitor password=secret"
#
#   Environment variable mode:
#     PGHOST=dbhost PGPORT=5432 PGDATABASE=postgres PGUSER=monitor PGPASSWORD=secret ./pg_health_check.sh
#
# Output:
# - A timestamped text report is written under:
#     ./reports/
#
# Notes:
# - Database name defaults to 'postgres' in interactive mode.
# - Password is entered hidden in interactive mode.
# - Query text shown from pg_stat_statements may still be limited by PostgreSQL
#   server-side tracking settings in some environments.
#

set -euo pipefail

usage() {
  cat <<'EOF'
PostgreSQL Health Check Script

Usage:
  ./pg_health_check.sh
  ./pg_health_check.sh -d "host=dbhost port=5432 dbname=postgres user=monitor password=secret"

Options:
  -d    PostgreSQL libpq connection string

Examples:
  ./pg_health_check.sh

  ./pg_health_check.sh -d "host=prod-db port=5432 dbname=postgres user=monitor password=secret"

  PGHOST=prod-db PGPORT=5432 PGDATABASE=postgres PGUSER=monitor PGPASSWORD=secret ./pg_health_check.sh

What the script checks:
  1. Version and basic instance info
  2. Potentially unused indexes
  3. Possible duplicate indexes
  4. High sequential scan tables (index candidates)
  5. Large tables with dead tuples > 20% and size > 1GB
  6. Transaction ID wraparound risk
  7. Top resource-consuming queries
  8. Long-running query patterns
  9. Connections and cache health

Important:
  - This script is read-only and does not change database objects.
  - All findings should be validated before remediation.
EOF
}

CONN_STR=""

while getopts "d:h" opt; do
  case "$opt" in
    d) CONN_STR="$OPTARG" ;;
    h) usage; exit 0 ;;
    *) usage; exit 1 ;;
  esac
done

if [[ -z "$CONN_STR" ]]; then
  echo "PostgreSQL connection details:"
  read -r -p "Host (default: localhost): " PGHOST_IN
  PGHOST_IN=${PGHOST_IN:-localhost}

  read -r -p "Port (default: 5432): " PGPORT_IN
  PGPORT_IN=${PGPORT_IN:-5432}

  read -r -p "Database name (default: postgres): " PGDATABASE_IN
  PGDATABASE_IN=${PGDATABASE_IN:-postgres}

  read -r -p "Username: " PGUSER_IN
  read -r -s -p "Password: " PGPASSWORD_IN
  echo

  export PGHOST="$PGHOST_IN"
  export PGPORT="$PGPORT_IN"
  export PGDATABASE="$PGDATABASE_IN"
  export PGUSER="$PGUSER_IN"
  export PGPASSWORD="$PGPASSWORD_IN"

  CONN_STR="host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER password=$PGPASSWORD"
fi

DATE_STR="$(date +%Y%m%d_%H%M%S)"
REPORT_DIR="${REPORT_DIR:-./reports}"
mkdir -p "$REPORT_DIR"
REPORT_FILE="${REPORT_DIR}/pg_health_report_${DATE_STR}.txt"

psql_base() {
  psql -q -X -v ON_ERROR_STOP=1 "$CONN_STR" "$@"
}

COMMON_PSQL_SETTINGS=$(cat <<'EOS'
\set QUIET 1
\pset pager off
\pset border 2
\pset null '<null>'
\pset linestyle ascii
\pset columns 140
\x auto
EOS
)

WRAPPED_PSQL_SETTINGS=$(cat <<'EOS'
\set QUIET 1
\pset pager off
\pset border 0
\pset null '<null>'
\pset linestyle ascii
\pset format wrapped
\pset columns 140
\t on
\x off
EOS
)

banner() {
  local title="$1"
  {
    echo
    echo "======================================================================"
    echo "== $title"
    echo "======================================================================"
    echo
  } >> "$REPORT_FILE"
}

recommendation() {
  {
    echo
    echo "Recommendation:"
    while IFS= read -r line; do
      echo "- $line"
    done
    echo
  } >> "$REPORT_FILE"
}

echo "PostgreSQL Health Check Report" > "$REPORT_FILE"
echo "Generated at: $(date)" >> "$REPORT_FILE"
echo "Target: host=${PGHOST:-localhost} db=${PGDATABASE:-postgres} user=${PGUSER:-unknown}" >> "$REPORT_FILE"
echo "======================================================================" >> "$REPORT_FILE"

########################################
# 1. Version & basic info
########################################
banner "1) Version & Basic Info"

{
cat <<SQL
$COMMON_PSQL_SETTINGS

SELECT current_database() AS db_name,
       version() AS version,
       pg_postmaster_start_time() AS postmaster_start_time,
       current_setting('max_connections') AS max_connections,
       current_setting('shared_buffers') AS shared_buffers;

SELECT datname,
       pg_size_pretty(pg_database_size(datname)) AS size
FROM pg_database
ORDER BY pg_database_size(datname) DESC;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
Ensure you are on a supported PostgreSQL major version.
Review max_connections versus connection pooling such as PgBouncer.
Validate shared_buffers and other core memory settings against server RAM and workload.
EOF

########################################
# 2. Potentially unused indexes
########################################
banner "2) Potentially Unused / Low-Usage Indexes"

{
cat <<SQL
$COMMON_PSQL_SETTINGS

SELECT
    s.schemaname,
    s.relname AS table_name,
    s.indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan,
    s.idx_tup_read,
    s.idx_tup_fetch
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE NOT i.indisprimary
  AND NOT i.indisunique
  AND pg_relation_size(s.indexrelid) > 50 * 1024 * 1024
  AND s.idx_scan < 50
ORDER BY pg_relation_size(s.indexrelid) DESC, s.idx_scan ASC
LIMIT 100;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
Review these indexes before dropping them.
Validate against actual workload and application usage because some indexes are used only during specific jobs or month-end processing.
Prefer dropping the largest truly unused indexes first.
EOF

########################################
# 3. Duplicate indexes
########################################
banner "3) Possible Duplicate Indexes"

{
cat <<SQL
$COMMON_PSQL_SETTINGS

WITH indexes AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        i.indexrelid,
        ic.relname AS index_name,
        i.indkey,
        pg_relation_size(i.indexrelid) AS index_size_bytes
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
),
index_cols AS (
    SELECT
        schema_name,
        table_name,
        indexrelid,
        index_name,
        index_size_bytes,
        indkey::text AS indkey_text
    FROM indexes
)
SELECT
    a.schema_name,
    a.table_name,
    a.index_name AS idx1,
    b.index_name AS idx2,
    pg_size_pretty(a.index_size_bytes) AS idx1_size,
    pg_size_pretty(b.index_size_bytes) AS idx2_size
FROM index_cols a
JOIN index_cols b
  ON a.schema_name = b.schema_name
 AND a.table_name  = b.table_name
 AND a.indexrelid  < b.indexrelid
 AND a.indkey_text = b.indkey_text
ORDER BY a.schema_name, a.table_name, a.index_name;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
Review duplicate index pairs and remove redundant ones after validating uniqueness, constraints, and workload patterns.
Keep the index that best matches the most valuable query predicates and sort order.
EOF

########################################
# 4. High sequential scan tables
########################################
banner "4) High Sequential Scan Tables (Index Candidates)"

{
cat <<SQL
$COMMON_PSQL_SETTINGS

WITH seq AS (
  SELECT
    schemaname,
    relname AS table_name,
    seq_scan,
    seq_tup_read,
    CASE
      WHEN seq_scan = 0 THEN 0
      ELSE seq_tup_read::numeric / seq_scan
    END AS avg_tup_per_seq_scan,
    pg_relation_size(relid) AS table_bytes
  FROM pg_stat_all_tables
  WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
)
SELECT
  schemaname,
  table_name,
  seq_scan,
  seq_tup_read,
  round(avg_tup_per_seq_scan, 2) AS avg_tup_per_seq_scan,
  pg_size_pretty(table_bytes) AS table_size
FROM seq
WHERE seq_scan > 100
  AND seq_tup_read > 100000
ORDER BY avg_tup_per_seq_scan DESC, table_bytes DESC
LIMIT 50;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
Investigate queries against these tables to determine whether indexes are missing or ineffective.
High sequential scans are acceptable for ETL, reporting, and full-table workloads, so verify the access pattern before adding indexes.
EOF

########################################
# 5. Large tables with high dead tuples
########################################
banner "5) Large Tables with Dead Tuples > 20% and Size > 1GB"

{
cat <<SQL
$WRAPPED_PSQL_SETTINGS

WITH stats AS (
  SELECT
    schemaname,
    relname AS table_name,
    n_live_tup,
    n_dead_tup,
    pg_relation_size(relid) AS table_bytes,
    last_autovacuum,
    last_autoanalyze,
    autovacuum_count,
    autoanalyze_count,
    round(
      100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0),
      2
    ) AS dead_pct
  FROM pg_stat_user_tables
)
SELECT format(
'TABLE       : %s.%s
SIZE        : %s
LIVE ROWS   : %s
DEAD ROWS   : %s
DEAD %%      : %s
LAST AV      : %s
LAST ANALYZE : %s
AV COUNT     : %s
ANALYZE CNT  : %s
%s',
  schemaname,
  table_name,
  pg_size_pretty(table_bytes),
  n_live_tup,
  n_dead_tup,
  dead_pct,
  coalesce(to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI:SS'), '<never>'),
  coalesce(to_char(last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS'), '<never>'),
  autovacuum_count,
  autoanalyze_count,
  repeat('-', 70)
)
FROM stats
WHERE table_bytes > 1024 * 1024 * 1024
  AND n_dead_tup > 0
  AND dead_pct > 20
ORDER BY dead_pct DESC, table_bytes DESC
LIMIT 50;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
Review these large tables for autovacuum lag and bloat growth.
If dead tuple percentage remains high, tune autovacuum thresholds per table or schedule manual VACUUM during low-activity windows.
Use VACUUM FULL only when reclaiming space is essential and maintenance downtime is acceptable.
EOF

########################################
# 6. Transaction ID wraparound risk
########################################
banner "6) Transaction ID Wraparound Risk"

{
cat <<SQL
$COMMON_PSQL_SETTINGS

SELECT
  datname,
  age(datfrozenxid) AS xid_age,
  pg_size_pretty(pg_database_size(datname)) AS db_size,
  CASE
    WHEN age(datfrozenxid) >= 1900000000 THEN 'CRITICAL: near 2B, vacuum immediately'
    WHEN age(datfrozenxid) >= 1500000000 THEN 'WARNING: plan aggressive VACUUM FREEZE'
    WHEN age(datfrozenxid) >= 1000000000 THEN 'NOTICE: monitor and ensure regular vacuum'
    ELSE 'OK'
  END AS risk_level
FROM pg_database
ORDER BY age(datfrozenxid) DESC
LIMIT 20;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
Prioritize VACUUM FREEZE for databases in WARNING or CRITICAL state.
Investigate tables with old frozen XIDs and confirm autovacuum is not blocked or disabled.
EOF

########################################
# 7. Top queries
########################################
banner "7) Top Resource-Consuming Queries"

{
cat <<SQL
$WRAPPED_PSQL_SETTINGS

SELECT format(
'QUERY ID    : %s
TOTAL MS    : %s
CALLS       : %s
AVG MS      : %s
QUERY       : %s
%s',
  queryid,
  round(total_exec_time::numeric, 2),
  calls,
  round((total_exec_time / calls)::numeric, 2),
  regexp_replace(query, '\s+', ' ', 'g'),
  repeat('-', 70)
)
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE" || {
  echo "Note: pg_stat_statements not available or not accessible." >> "$REPORT_FILE"
}

recommendation <<'EOF'
Prioritize queries with both high total execution time and high average execution time.
Use queryid to track tuning work before and after changes.
Reset pg_stat_statements daily if you want this section to represent roughly the last 24 hours.
EOF

########################################
# 8. Long-running query patterns
########################################
banner "8) Long-Running Query Patterns"

{
cat <<SQL
$WRAPPED_PSQL_SETTINGS

SELECT format(
'QUERY ID    : %s
AVG MS      : %s
CALLS       : %s
TOTAL MS    : %s
QUERY       : %s
%s',
  queryid,
  round(mean_exec_time::numeric, 2),
  calls,
  round(total_exec_time::numeric, 2),
  regexp_replace(query, '\s+', ' ', 'g'),
  repeat('-', 70)
)
FROM pg_stat_statements
WHERE mean_exec_time > 1000
  AND calls >= 10
ORDER BY mean_exec_time DESC
LIMIT 20;
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE" || {
  echo "Note: pg_stat_statements not available or not accessible." >> "$REPORT_FILE"
}

recommendation <<'EOF'
Focus first on patterns with high average execution time, especially those called frequently.
Review execution plans, missing predicates, sort operations, and large join paths for these statements.
EOF

########################################
# 9. Connections and cache health
########################################
banner "9) Connections & Cache Health"

{
cat <<SQL
$COMMON_PSQL_SETTINGS

SELECT
    datname,
    count(*) AS num_connections,
    count(*) FILTER (WHERE state = 'active') AS active_connections
FROM pg_stat_activity
GROUP BY datname
ORDER BY num_connections DESC;

SELECT
    sum(blks_hit) AS blks_hit,
    sum(blks_read) AS blks_read,
    round(
      100.0 * sum(blks_hit) / NULLIF(sum(blks_hit) + sum(blks_read), 0),
      2
    ) AS cache_hit_ratio
FROM pg_stat_database
WHERE datname = current_database();
SQL
} | psql_base >> "$REPORT_FILE" 2>>"$REPORT_FILE"

recommendation <<'EOF'
If active or total connections are consistently high, use pooling and review idle session behavior.
For OLTP systems, a low cache hit ratio often indicates memory pressure, poor locality, or inefficient query patterns.
EOF

echo
echo "Report written to: $REPORT_FILE"
