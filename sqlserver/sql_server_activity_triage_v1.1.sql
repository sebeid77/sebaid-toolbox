/* =========================================================
   README / DISCLAIMER
   =========================================================
   Script:   sql_server_activity_triage_v1.1.sql
   Author:   Sameh Ebeid
   Purpose:  Quick operational triage for non-DBA users

   What this script does:
   1) Reviews key instance settings (memory / MAXDOP / cost threshold)
   2) Counts user connections by status
   3) Detects blocking chains and head blockers
   4) Shows current activity with SQL text and execution plan
   5) Highlights long-running requests (> 5 minutes)
   6) Shows top 5 wait stats
   7) Checks statistics freshness across all online non-system databases
   8) Checks Availability Group synchronization and lag (if AG is enabled)

   Intended audience:
   - Non-DBA support staff
   - Application owners
   - Operations teams
   - Junior DBAs

   Safety notes:
   - This script is read-only.
   - It does NOT change server settings.
   - It does NOT kill sessions automatically.
   - Any KILL command shown is for manual review only.
   - Always confirm with the application owner or DBA before killing a session.

   Permissions:
   - VIEW SERVER STATE is recommended for complete results.
   - VIEW DATABASE STATE may be required for some database-level details.

   Notes:
   - Sections 3-5 focus on user workload and exclude:
     master, model, msdb, tempdb, and rdsadmin.
   - Section 8 checks all online non-system databases.
   - Section 9 is only relevant if the instance participates in an Always On Availability Group.
   - AG lag recommendations are heuristics from DMV snapshots, not absolute proof of root cause.

   Thresholds used in this script:
   - Stats stale warning: 7 days
   - Synchronous AG lag warning: 30 seconds
   - Asynchronous AG lag warning: 300 seconds
   - Long-running request threshold: 5 minutes

   Disclaimer:
   - This script is provided AS IS with no warranty.
   - Validate all findings before taking action in production.
   ========================================================= */

SET NOCOUNT ON;
GO

/***********************************************************
 How to read this output
***********************************************************/
PRINT '=== How to read this script ===';
PRINT '1) Start with configuration, blocking, active requests, and wait stats for broad clues.';
PRINT '2) If users report slowness, check blocking, long-running queries, and current activity first.';
PRINT '3) Review statistics freshness after workload checks if plans or estimates seem suspicious.';
PRINT '4) If this server uses AGs, review sync state and lag before failover decisions.';
PRINT '5) If plans look suspicious, copy the plan XML to PasteThePlan for another opinion.';
GO

/***********************************************************
 1) Key config: max memory, MAXDOP, cost threshold
***********************************************************/
PRINT '=== 1) Key configuration (max memory, MAXDOP, cost threshold) ===';

;WITH cfg AS (
    SELECT 
        name,
        value_in_use,
        CAST(value_in_use AS int) AS value_int
    FROM sys.configurations
    WHERE name IN (
        'max server memory (MB)',
        'max degree of parallelism',
        'cost threshold for parallelism'
    )
)
SELECT
    c.name,
    c.value_in_use,
    d.default_value,
    CASE c.name
        WHEN 'max server memory (MB)' THEN
            CASE 
                WHEN c.value_int >= 2147483647 THEN 
                    'Default (unlimited). A DBA should usually set max server memory so Windows and other services keep enough RAM.'
                WHEN c.value_int < 2048 THEN 
                    'Very low max memory. If the server is dedicated to SQL Server, ask a DBA to review whether memory is set too low.'
                ELSE 
                    'Configured value looks intentional. If memory pressure exists, a DBA should compare this with total server RAM and workload needs.'
            END
        WHEN 'max degree of parallelism' THEN
            CASE 
                WHEN c.value_int = 0 THEN 
                    'MAXDOP = 0 means SQL Server can use all available schedulers for parallel queries. This may be fine, but a DBA should validate CPU and wait stats.'
                WHEN c.value_int = 1 THEN 
                    'MAXDOP = 1 disables parallelism. This can be valid for some workloads, but a DBA should confirm it is intentional.'
                ELSE 
                    'MAXDOP is set to a custom value. A DBA should confirm it matches core count, NUMA layout, and workload type.'
            END
        WHEN 'cost threshold for parallelism' THEN
            CASE 
                WHEN c.value_int <= 5 THEN 
                    'Cost threshold is at or near the default of 5, which is often too low on modern systems. A DBA may want to review this if CPU or parallelism waits are high.'
                WHEN c.value_int BETWEEN 6 AND 29 THEN 
                    'Cost threshold is above default but still relatively low. Review with a DBA if many small queries are going parallel.'
                ELSE 
                    'Cost threshold is set above 30, which is common in tuned environments. Confirm with workload and wait stats.'
            END
    END AS recommendation
FROM cfg AS c
OUTER APPLY (
    VALUES
      ('max server memory (MB)', 2147483647),
      ('max degree of parallelism', 0),
      ('cost threshold for parallelism', 5)
) AS d(name, default_value)
WHERE d.name = c.name
ORDER BY c.name;
GO

/***********************************************************
 2) Connection count by status (all databases)
***********************************************************/
PRINT '=== 2) Connection count by status (all databases) ===';

SELECT
    s.status,
    COUNT(*) AS session_count,
    CASE
        WHEN s.status = 'running' THEN
            'Running sessions are actively using CPU right now. A high count may indicate heavy workload or concurrency pressure.'
        WHEN s.status = 'sleeping' THEN
            'Sleeping sessions are connected but not currently executing work. This is often normal for connection pooling or idle sessions.'
        WHEN s.status = 'suspended' THEN
            'Suspended sessions are waiting on a resource such as locks, IO, memory, or network. If this count is high, review blocking and wait stats.'
        WHEN s.status = 'runnable' THEN
            'Runnable sessions are ready to run but waiting for CPU time. A high count may indicate CPU pressure.'
        ELSE
            'Review this status if counts are high or unexpected.'
    END AS recommendation
FROM sys.dm_exec_sessions s
WHERE s.is_user_process = 1
  AND s.session_id <> @@SPID
GROUP BY s.status
ORDER BY session_count DESC;
GO

/***********************************************************
 3) Blocking and head blocker (user DBs only)
    Includes open transaction count
***********************************************************/
PRINT '=== 3) Blocking and head blocker (user databases only) ===';

;WITH blocking_data AS (
    SELECT
        r.session_id,
        r.blocking_session_id,
        r.status,
        r.wait_type,
        r.wait_time / 1000.0 AS wait_seconds,
        r.wait_resource,
        r.database_id,
        s.login_name,
        s.host_name,
        s.program_name,
        s.open_transaction_count,
        st.text AS sql_text
    FROM sys.dm_exec_requests r
    JOIN sys.dm_exec_sessions s
      ON r.session_id = s.session_id
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
    WHERE r.session_id <> @@SPID
      AND r.database_id IS NOT NULL
      AND DB_NAME(r.database_id) NOT IN ('master','model','msdb','tempdb','rdsadmin')
),
head_blockers AS (
    SELECT DISTINCT
        b.blocking_session_id AS head_blocker_session_id
    FROM blocking_data b
    WHERE b.blocking_session_id > 0
      AND b.blocking_session_id NOT IN (
            SELECT session_id
            FROM blocking_data
            WHERE blocking_session_id > 0
      )
),
tran_counts AS (
    SELECT
        session_id,
        MAX(open_transaction_count) AS tran_open_count
    FROM sys.dm_tran_session_transactions
    GROUP BY session_id
)
SELECT
    b.session_id,
    b.blocking_session_id,
    DB_NAME(b.database_id) AS database_name,
    b.status,
    b.wait_type,
    b.wait_seconds,
    b.wait_resource,
    b.login_name,
    b.host_name,
    b.program_name,
    ISNULL(tc.tran_open_count, b.open_transaction_count) AS open_transaction_count,
    LEFT(REPLACE(REPLACE(b.sql_text, CHAR(10), ' '), CHAR(13), ' '), 4000) AS sql_text,
    CASE
        WHEN hb.head_blocker_session_id IS NOT NULL AND ISNULL(tc.tran_open_count, b.open_transaction_count) > 0 THEN
            'Head blocker with open transaction(s). This is higher risk because the session may be holding locks even if application activity appears idle.'
        WHEN hb.head_blocker_session_id IS NOT NULL THEN
            'Head blocker detected. Review the SQL text, application owner, and business impact before taking action.'
        ELSE
            'Blocked session. Focus on the head blocker rather than killing blocked victim sessions.'
    END AS recommendation,
    CASE
        WHEN hb.head_blocker_session_id IS NOT NULL THEN
            'KILL ' + CAST(b.session_id AS varchar(20)) + '  -- Manual use only. Confirm impact first.'
        ELSE
            NULL
    END AS kill_command
FROM blocking_data b
LEFT JOIN head_blockers hb
  ON b.session_id = hb.head_blocker_session_id
LEFT JOIN tran_counts tc
  ON b.session_id = tc.session_id
WHERE b.blocking_session_id > 0
   OR hb.head_blocker_session_id IS NOT NULL
ORDER BY
    CASE
        WHEN hb.head_blocker_session_id IS NOT NULL AND ISNULL(tc.tran_open_count, b.open_transaction_count) > 0 THEN 0
        WHEN hb.head_blocker_session_id IS NOT NULL THEN 1
        ELSE 2
    END,
    b.wait_seconds DESC;
GO

/***********************************************************
 4) Current running activity (WhoIsActive-style snapshot)
     Includes execution plan
***********************************************************/
PRINT '=== 4) Current running activity (user databases only, with execution plan) ===';

SELECT
    r.session_id,
    DB_NAME(r.database_id) AS database_name,
    s.status AS session_status,
    r.status AS request_status,
    s.login_name,
    s.host_name,
    s.program_name,
    r.command,
    r.cpu_time,
    r.logical_reads,
    r.reads,
    r.writes,
    r.wait_type,
    r.wait_time / 1000.0 AS wait_seconds,
    r.blocking_session_id,
    DATEDIFF(SECOND, r.start_time, SYSDATETIME()) AS running_seconds,
    LEFT(REPLACE(REPLACE(st.text, CHAR(10), ' '), CHAR(13), ' '), 4000) AS current_sql_text,
    qp.query_plan AS execution_plan,
    CASE
        WHEN DATEDIFF(SECOND, r.start_time, SYSDATETIME()) < 30 THEN
            'Currently running, but runtime is still short. Monitor if it continues to grow. If you need help, copy the execution plan XML and paste it at https://www.brentozar.com/pastetheplan/, press Submit, then review the AI Suggestions tab.'
        WHEN DATEDIFF(SECOND, r.start_time, SYSDATETIME()) BETWEEN 30 AND 300 THEN
            'Moderate runtime. If there is a performance concern, review the wait type, reads, and potential blocking. For additional help, copy the execution plan XML and paste it at https://www.brentozar.com/pastetheplan/, press Submit, then review the AI Suggestions tab.'
        WHEN DATEDIFF(SECOND, r.start_time, SYSDATETIME()) > 300 THEN
            'Long-running active request (> 5 minutes). If this is unexpected, review the execution plan and involve a DBA. You can also copy the execution plan XML into https://www.brentozar.com/pastetheplan/, press Submit, and review the AI Suggestions tab for ideas to improve the query.'
        ELSE
            'Review session details if this activity is unexpected. You can copy the execution plan XML and paste it at https://www.brentozar.com/pastetheplan/, press Submit, then review the AI Suggestions tab for additional guidance.'
    END AS recommendation
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions s
  ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) qp
WHERE s.is_user_process = 1
  AND r.session_id <> @@SPID
  AND r.database_id IS NOT NULL
  AND DB_NAME(r.database_id) NOT IN ('master','model','msdb','tempdb','rdsadmin')
ORDER BY running_seconds DESC, r.cpu_time DESC;
GO

/***********************************************************
 5) Long-running requests over 5 minutes (user DBs only)
***********************************************************/
PRINT '=== 5) Long-running requests over 5 minutes (user databases only) ===';

SELECT
    r.session_id,
    DB_NAME(r.database_id) AS database_name,
    s.login_name,
    s.host_name,
    s.program_name,
    r.status,
    r.command,
    r.wait_type,
    r.blocking_session_id,
    r.cpu_time,
    r.logical_reads,
    r.reads,
    r.writes,
    DATEDIFF(MINUTE, r.start_time, SYSDATETIME()) AS running_minutes,
    LEFT(REPLACE(REPLACE(st.text, CHAR(10), ' '), CHAR(13), ' '), 4000) AS sql_text,
    CASE
        WHEN r.blocking_session_id > 0 THEN
            'This request has been running more than 5 minutes and is blocked. Investigate and resolve the blocker before considering a KILL.'
        WHEN r.wait_type IS NOT NULL THEN
            'This request has been running more than 5 minutes. Involve a DBA to review the wait type and execution plan for IO, CPU, memory, or locking pressure.'
        ELSE
            'This request has been running more than 5 minutes with no obvious blocking shown. Involve a DBA to review whether the runtime is expected.'
    END AS recommendation
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions s
  ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
WHERE s.is_user_process = 1
  AND r.session_id <> @@SPID
  AND DATEDIFF(MINUTE, r.start_time, SYSDATETIME()) >= 5
  AND r.database_id IS NOT NULL
  AND DB_NAME(r.database_id) NOT IN ('master','model','msdb','tempdb','rdsadmin')
ORDER BY running_minutes DESC, r.cpu_time DESC;
GO

/***********************************************************
 6) Top 5 wait stats (instance-wide)
***********************************************************/
PRINT '=== 6) Top 5 wait stats (instance-wide) ===';

;WITH waits AS (
    SELECT 
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        signal_wait_time_ms,
        wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        'BROKER_EVENTHANDLER','BROKER_RECEIVE_WAITFOR','BROKER_TASK_STOP',
        'BROKER_TO_FLUSH','BROKER_TRANSMITTER','CHECKPOINT_QUEUE',
        'CLR_AUTO_EVENT','CLR_MANUAL_EVENT','CLR_SEMAPHORE',
        'LAZYWRITER_SLEEP','RESOURCE_QUEUE','SLEEP_TASK','SLEEP_SYSTEMTASK',
        'SQLTRACE_BUFFER_FLUSH','XE_DISPATCHER_WAIT','XE_TIMER_EVENT',
        'XE_DISPATCHER_JOIN','WAITFOR','LOGMGR_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH',
        'FT_IFTS_SCHEDULER_IDLE_WAIT','FT_IFTSHC_MUTEX','SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        'BROKER_CONNECTION_RECEIVE_TASK','ONDEMAND_TASK_QUEUE','DBMIRROR_EVENTS_QUEUE',
        'DBMIRRORING_CMD','HADR_FILESTREAM_IOMGR_IOCOMPLETION','DISPATCHER_QUEUE_SEMAPHORE',
        'HADR_CLUSAPI_CALL','HADR_LOGCAPTURE_WAIT','HADR_NOTIFICATION_DEQUEUE',
        'HADR_TIMER_TASK','HADR_WORK_QUEUE','DIRTY_PAGE_POLL','QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
        'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP','QDS_SHUTDOWN_QUEUE','PWAIT_ALL_COMPONENTS_INITIALIZED'
    )
),
top_waits AS (
    SELECT TOP (5)
        wait_type,
        waiting_tasks_count,
        wait_time_ms / 1000.0 AS wait_seconds,
        signal_wait_time_ms / 1000.0 AS signal_wait_seconds,
        resource_wait_time_ms / 1000.0 AS resource_wait_seconds
    FROM waits
    WHERE wait_time_ms > 0
    ORDER BY wait_time_ms DESC
)
SELECT
    wait_type,
    waiting_tasks_count,
    wait_seconds,
    signal_wait_seconds,
    resource_wait_seconds,
    CASE 
        WHEN wait_type LIKE 'LCK_M_%' THEN
            'Lock waits indicate blocking. Check section 3, and for deeper guidance see wait statistics references from Paul Randal and Brent Ozar.'
        WHEN wait_type LIKE 'PAGEIOLATCH_%' THEN
            'High IO waits suggest slow storage or large scans. Consider indexing and IO review, then compare findings with active requests and long-running queries.'
        WHEN wait_type IN ('CXPACKET','CXCONSUMER') THEN
            'Parallelism waits may indicate inefficient parallel plans. A DBA can review MAXDOP, cost threshold, and query design.'
        WHEN wait_type = 'SOS_SCHEDULER_YIELD' THEN
            'CPU pressure may be present. Review active and long-running queries, then compare with runnable tasks and workload timing.'
        WHEN wait_type LIKE 'WRITELOG' OR wait_type LIKE 'LOG%' THEN
            'Log-related waits can point to transaction log throughput issues or very frequent commits. Involve a DBA and compare with AG send lag if this server participates in AGs.'
        ELSE
            'Review this wait type in context with workload and other sections of this report. Wait stats are most useful when combined with blocking, query activity, and storage findings.'
    END AS recommendation
FROM top_waits
ORDER BY wait_seconds DESC;
GO

/***********************************************************
 7) Statistics freshness (all online non-system databases)
     Summary only
***********************************************************/
PRINT '=== 7) Statistics freshness (all online non-system databases) ===';

IF OBJECT_ID('tempdb..#StatsInfo') IS NOT NULL
    DROP TABLE #StatsInfo;

CREATE TABLE #StatsInfo
(
    database_name               sysname,
    schema_name                 sysname,
    table_name                  sysname,
    stats_name                  sysname,
    last_updated                datetime NULL,
    rows_count                  bigint NULL,
    rows_sampled                bigint NULL,
    modification_counter        bigint NULL
);

DECLARE @db_name sysname;
DECLARE @sql nvarchar(max);

DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND state_desc = 'ONLINE'
  AND name <> 'rdsadmin';

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
    USE ' + QUOTENAME(@db_name) + N';

    INSERT INTO #StatsInfo
    (
        database_name,
        schema_name,
        table_name,
        stats_name,
        last_updated,
        rows_count,
        rows_sampled,
        modification_counter
    )
    SELECT
        DB_NAME() AS database_name,
        OBJECT_SCHEMA_NAME(s.object_id) AS schema_name,
        OBJECT_NAME(s.object_id) AS table_name,
        s.name AS stats_name,
        sp.last_updated,
        sp.rows,
        sp.rows_sampled,
        sp.modification_counter
    FROM sys.stats AS s
    CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
    WHERE OBJECTPROPERTY(s.object_id, ''IsUserTable'') = 1;
    ';

    BEGIN TRY
        EXEC sys.sp_executesql @sql;
    END TRY
    BEGIN CATCH
        INSERT INTO #StatsInfo
        (
            database_name,
            schema_name,
            table_name,
            stats_name,
            last_updated,
            rows_count,
            rows_sampled,
            modification_counter
        )
        VALUES
        (
            @db_name,
            'ERROR',
            'Unable to read statistics in this database',
            ERROR_MESSAGE(),
            NULL,
            NULL,
            NULL,
            NULL
        );
    END CATCH;

    FETCH NEXT FROM db_cursor INTO @db_name;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

SELECT
    database_name,
    MIN(last_updated) AS oldest_stats_update,
    MAX(last_updated) AS newest_stats_update,
    COUNT(*) AS stats_count,
    SUM(CASE WHEN last_updated IS NULL THEN 1 ELSE 0 END) AS stats_never_updated_count,
    SUM(CASE WHEN modification_counter > 1000 THEN 1 ELSE 0 END) AS stats_with_high_modifications,
    CASE
        WHEN MIN(schema_name) = 'ERROR' THEN
            'Could not read one or more statistics in this database. Review permissions or database accessibility.'
        WHEN COUNT(*) = 0 THEN
            'No user-table statistics were found in this database.'
        WHEN MIN(last_updated) IS NULL THEN
            'Some statistics have never been updated or returned no date. This can happen on empty tables or when no histogram blob exists yet.'
        WHEN MIN(last_updated) < DATEADD(DAY, -7, SYSDATETIME()) THEN
            'Some statistics are older than 7 days. If performance issues exist, ask a DBA whether manual stats maintenance or auto-update thresholds need review.'
        ELSE
            'Statistics look reasonably current (within the last 7 days).'
    END AS recommendation
FROM #StatsInfo
GROUP BY database_name
ORDER BY
    CASE
        WHEN MIN(schema_name) = 'ERROR' THEN 0
        WHEN MIN(last_updated) IS NULL THEN 1
        WHEN MIN(last_updated) < DATEADD(DAY, -7, SYSDATETIME()) THEN 2
        ELSE 3
    END,
    oldest_stats_update ASC;

DROP TABLE #StatsInfo;
GO
/***********************************************************
 8) Availability Group synchronization and lag
***********************************************************/
PRINT '=== 8) Availability Group synchronization and sync lag (if AG is enabled) ===';

IF NOT EXISTS (SELECT 1 FROM sys.dm_hadr_availability_group_states)
BEGIN
    SELECT
        'This instance does not appear to be part of an Always On Availability Group, or AG is not enabled.' AS info,
        'If you expected AGs here, involve a DBA to confirm AG configuration, cluster state, and endpoint connectivity.' AS recommendation;
END
ELSE
BEGIN
    ;WITH ag_lag AS
    (
        SELECT
            ag.name                         AS availability_group_name,
            ar.replica_server_name          AS replica_server_name,
            DB_NAME(drs.database_id)        AS database_name,
            ars.role_desc                   AS replica_role,
            ar.availability_mode_desc       AS availability_mode,
            drs.synchronization_state_desc  AS sync_state,
            drs.synchronization_health_desc AS sync_health,
            drs.log_send_queue_size,        -- KB
            drs.log_send_rate,              -- KB/sec
            drs.redo_queue_size,            -- KB
            drs.redo_rate,                  -- KB/sec
            drs.last_hardened_time,
            drs.last_redone_time,
            drs.last_commit_time,
            DATEDIFF(SECOND, drs.last_commit_time, SYSDATETIME()) AS commit_lag_seconds
        FROM sys.availability_groups AS ag
        JOIN sys.availability_replicas AS ar
            ON ag.group_id = ar.group_id
        JOIN sys.dm_hadr_availability_replica_states AS ars
            ON ar.replica_id = ars.replica_id
        JOIN sys.dm_hadr_database_replica_states AS drs
            ON drs.group_id = ag.group_id
           AND drs.replica_id = ars.replica_id
        WHERE ars.is_local = 0
          AND drs.is_primary_replica = 0
    )
    SELECT
        availability_group_name,
        replica_server_name,
        database_name,
        replica_role,
        availability_mode,
        sync_state,
        sync_health,
        log_send_queue_size,
        log_send_rate,
        redo_queue_size,
        redo_rate,
        commit_lag_seconds,
        last_hardened_time,
        last_redone_time,
        last_commit_time,
        CASE
            WHEN sync_state NOT IN ('SYNCHRONIZED','SYNCHRONIZING') THEN
                'Replica is not synchronized. Involve a DBA to check AG health, cluster state, endpoint connectivity, and failover readiness.'
            WHEN availability_mode = 'SYNCHRONOUS_COMMIT'
                 AND commit_lag_seconds > 30
                 AND ISNULL(log_send_queue_size,0) = 0
                 AND ISNULL(redo_queue_size,0) > 0 THEN
                'Most log has already been sent, but redo is behind on the secondary. This often points to slow secondary IO, redo pressure, or heavy recovery work. Ask a DBA to review storage latency and redo throughput.'
            WHEN availability_mode = 'SYNCHRONOUS_COMMIT'
                 AND commit_lag_seconds > 30
                 AND ISNULL(log_send_queue_size,0) > 0
                 AND ISNULL(redo_queue_size,0) = 0 THEN
                'Log is backing up before or during send to the secondary. This often suggests network delay, send path issues, or primary log throughput pressure. Ask a DBA to review network latency and primary log write performance.'
            WHEN availability_mode = 'SYNCHRONOUS_COMMIT'
                 AND commit_lag_seconds > 30
                 AND ISNULL(log_send_queue_size,0) > 0
                 AND ISNULL(redo_queue_size,0) > 0 THEN
                'Both send and redo queues are elevated. This can happen with heavy write workload, network delay, or slow secondary IO. Involve a DBA to review AG transport, redo throughput, and storage performance.'
            WHEN availability_mode = 'ASYNCHRONOUS_COMMIT'
                 AND commit_lag_seconds > 300 THEN
                'Asynchronous replica is behind by more than about 5 minutes. Confirm whether this is acceptable for your recovery objectives. If not, ask a DBA to review send and redo bottlenecks.'
            ELSE
                'Synchronization state and lag look normal for this snapshot. If there are concerns, compare queue sizes and rates over time instead of relying on a single sample.'
        END AS recommendation
    FROM ag_lag
    ORDER BY availability_group_name, replica_server_name, database_name;
END;
GO
