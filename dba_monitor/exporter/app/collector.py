from typing import Any, Dict

import psycopg2
from prometheus_client import Gauge, CollectorRegistry


Q_CONNECTIONS = """
SELECT
  count(*) FILTER (WHERE state = 'active') AS active,
  count(*) FILTER (WHERE state = 'idle')   AS idle,
  count(*) AS total
FROM pg_stat_activity
WHERE pid <> pg_backend_pid();
"""

Q_LONG_RUNNING = """
SELECT count(*)
FROM pg_stat_activity
WHERE state = 'active'
  AND now() - query_start > interval '60 seconds'
  AND pid <> pg_backend_pid();
"""

Q_BLOCKED = """
SELECT count(*)
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
  AND wait_event IS NOT NULL
  AND state <> 'idle';
"""

Q_DEADLOCKS = """
SELECT coalesce(sum(deadlocks),0)
FROM pg_stat_database;
"""

Q_TPS = """
SELECT
  sum(xact_commit)   AS commits,
  sum(xact_rollback) AS rollbacks
FROM pg_stat_database;
"""

Q_CACHE_HIT = """
SELECT
  sum(blks_hit)  AS blks_hit,
  sum(blks_read) AS blks_read
FROM pg_stat_database;
"""

Q_DB_SIZE = """
SELECT pg_database_size(current_database());
"""


class AuroraPostgresCollector:
    def __init__(self, db_conf: Dict[str, Any]):
        self.db_conf = db_conf
        self.name = db_conf["name"]
        self.environment = db_conf.get("environment", "unknown")
        self.host = db_conf["host"]

    def collect_metrics(self, registry: CollectorRegistry) -> None:
        labels = {
            "database": self.name,
            "engine": "aurora_postgres",
            "environment": self.environment,
            "host": self.host,
        }

        g_up = Gauge(
            "dba_database_up",
            "Database up (1) / down (0)",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_status = Gauge(
            "dba_instance_status",
            "Overall status (1=ok,0=issue)",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_active_conn = Gauge(
            "dba_active_connections",
            "Active connections",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_idle_conn = Gauge(
            "dba_idle_connections",
            "Idle connections",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_total_conn = Gauge(
            "dba_total_connections",
            "Total connections",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_long_running = Gauge(
            "dba_long_running_queries",
            "Long running queries (>60s)",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_blocked = Gauge(
            "dba_blocked_sessions",
            "Blocked sessions (waiting on locks)",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_deadlocks = Gauge(
            "dba_deadlocks_total",
            "Total deadlocks (pg_stat_database)",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_commits = Gauge(
            "dba_commits_total",
            "Cumulative commits",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_rollbacks = Gauge(
            "dba_rollbacks_total",
            "Cumulative rollbacks",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_cache_hit = Gauge(
            "dba_cache_hit_ratio",
            "Approximate cache hit ratio",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )
        g_db_size = Gauge(
            "dba_database_size_bytes",
            "Database size (bytes)",
            ["database", "engine", "environment", "host"],
            registry=registry,
        )

        conn = None
        try:
            conn = psycopg2.connect(
                host=self.db_conf["host"],
                port=self.db_conf["port"],
                dbname=self.db_conf["database"],
                user=self.db_conf["user"],
                password=self.db_conf["password"],
                sslmode=self.db_conf.get("sslmode", "require"),
                connect_timeout=3,
            )
            conn.autocommit = True
            g_up.labels(**labels).set(1)

            status = 1  # assume ok, flip to 0 if we see issues

            with conn.cursor() as cur:
                # Connections
                cur.execute(Q_CONNECTIONS)
                active, idle, total = cur.fetchone()
                g_active_conn.labels(**labels).set(active)
                g_idle_conn.labels(**labels).set(idle)
                g_total_conn.labels(**labels).set(total)

                # Long running queries
                cur.execute(Q_LONG_RUNNING)
                long_running = cur.fetchone()[0]
                g_long_running.labels(**labels).set(long_running)
                if long_running > 0:
                    status = 0

                # Blocked sessions
                cur.execute(Q_BLOCKED)
                blocked = cur.fetchone()[0]
                g_blocked.labels(**labels).set(blocked)
                if blocked > 0:
                    status = 0

                # Deadlocks
                cur.execute(Q_DEADLOCKS)
                deadlocks = cur.fetchone()[0]
                g_deadlocks.labels(**labels).set(deadlocks)
                if deadlocks > 0:
                    status = 0

                # Commits / rollbacks
                cur.execute(Q_TPS)
                commits, rollbacks = cur.fetchone()
                g_commits.labels(**labels).set(commits)
                g_rollbacks.labels(**labels).set(rollbacks)

                # Cache hit ratio
                cur.execute(Q_CACHE_HIT)
                blks_hit, blks_read = cur.fetchone()
                ratio = (
                    float(blks_hit) / (blks_hit + blks_read)
                    if (blks_hit + blks_read) > 0
                    else 1.0
                )
                g_cache_hit.labels(**labels).set(ratio)
                if ratio < 0.9:
                    status = 0

                # Database size
                cur.execute(Q_DB_SIZE)
                g_db_size.labels(**labels).set(cur.fetchone()[0])

            g_status.labels(**labels).set(status)

        except Exception:
            g_up.labels(**labels).set(0)
            g_status.labels(**labels).set(0)
        finally:
            if conn:
                conn.close()
