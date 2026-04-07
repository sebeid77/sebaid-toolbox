/* =========================================================
   README / DISCLAIMER
   =========================================================
   Script:   SQL Server Instance Health Check v1
   Author:   Sameh Ebeid
   Purpose:  Quick-read instance health snapshot for SQL Server

   Sections:
   1) Instance version and last restart
   2) Key configuration (max memory, MAXDOP, cost threshold)
   3) Database compatibility level (user DBs)
   4) Dedicated Admin Connection (DAC) configuration
   5) Backup and recovery posture (skipped on RDS)
   6) Database state and basic integrity-related settings
   7) File autogrowth (user databases only, excludes rdsadmin)
      - Small   < 512 MB       ->  512 MB growth
      - Medium  512 MB–4 GB    -> 1024 MB (1 GB) growth
      - Large   4–8 GB         -> 4096 MB (4 GB) growth
      - XLarge  > 8 GB         -> 8192 MB (8 GB) growth
   8) Top 5 waits (with detailed explanation)
   9) Top 10 expensive queries (with text & plan)
   10) High-cost queries with missing-index suggestions
   11) Index usage – potential unused indexes
   12) Index fragmentation on large indexes (per database)

   IMPORTANT:
   - This script is provided "AS IS" with NO WARRANTY.
   - Use it ONLY in non-production first, review results carefully,
     and validate every recommendation before changing any settings.
   - I do NOT accept any responsibility or liability for any impact,
     data loss, downtime, or issues arising from running this script
     or applying any recommendations that result from it.

   By running this script, you accept full responsibility for any
   actions you take based on the output.
   ========================================================= */

SET NOCOUNT ON;

/***********************************************************
 1) Instance version and last restart
***********************************************************/
PRINT '=== 1) Instance version and last restart ===';

SELECT
    @@SERVERNAME                           AS server_name,
    SERVERPROPERTY('ProductVersion')       AS product_version,
    SERVERPROPERTY('ProductLevel')         AS product_level,
    SERVERPROPERTY('Edition')              AS edition,
    sqlserver_start_time                   AS last_restart_time,
    DATEDIFF(DAY, sqlserver_start_time, SYSDATETIME()) AS days_since_restart,
    CASE 
        WHEN DATEDIFF(DAY, sqlserver_start_time, SYSDATETIME()) > 60 
            THEN 'Consider scheduled restart in a maintenance window; long uptimes can hide config changes and accumulate log growth.'
        ELSE 'Uptime is reasonable; focus on workload and configuration first.'
    END AS recommendation,
    @@VERSION                              AS full_version_string  -- e.g. Microsoft SQL Server 2019 (RTM-CU32) (KB5054833)
FROM sys.dm_os_sys_info;
GO

/***********************************************************
 2) Key config: max memory, MAXDOP, cost threshold, LPIM, IFI
***********************************************************/
PRINT '=== 2) Key configuration (max memory, MAXDOP, cost threshold, LPIM, IFI) ===';

;WITH cfg AS (
    SELECT 
        name,
        value_in_use,
        CAST(value_in_use AS INT) AS value_int
    FROM sys.configurations
    WHERE name IN (
        'max server memory (MB)',
        'max degree of parallelism',
        'cost threshold for parallelism'
    )
),
base_config AS (
    SELECT
        c.name,
        CAST(c.value_in_use AS NVARCHAR(50)) AS current_value,
        CAST(d.default_value AS NVARCHAR(50)) AS default_value,
        CASE c.name
            WHEN 'max server memory (MB)' THEN
                CASE 
                    WHEN c.value_int >= 2147483647 THEN 
                        'Default (unlimited). Set max server memory so OS and other services always have free RAM; avoid paging.'
                    WHEN c.value_int < 2048 THEN 
                        'Very low max memory. Increase so SQL Server can cache data pages; review memory counters under peak load.'
                    ELSE 
                        'Non-default and looks reasonable. Validate with Target/Total Server Memory, PLE, and OS paging.'
                END
            WHEN 'max degree of parallelism' THEN
                CASE 
                    WHEN c.value_int = 0 THEN 
                        'MAXDOP = 0 (all schedulers). On NUMA / >8 cores this often causes excessive CXPACKET/CXCONSUMER waits; start with 4–8 for OLTP.'
                    WHEN c.value_int = 1 THEN 
                        'MAXDOP = 1 (no parallelism). Good for some OLTP workloads, but validate large reporting/ETL queries.'
                    ELSE 
                        'Custom MAXDOP. Ensure it does not exceed cores per NUMA node; correlate with parallelism waits.'
                END
            WHEN 'cost threshold for parallelism' THEN
                CASE 
                    WHEN c.value_int <= 5 THEN 
                        'Default 5 is usually too low on modern hardware. Raise to 30–50 so only expensive queries go parallel.'
                    WHEN c.value_int BETWEEN 6 AND 29 THEN 
                        'Higher than default but still low. Consider 30–50 depending on workload and observed wait stats.'
                    ELSE 
                        'Cost threshold already above 30. Confirm large reporting queries still parallelize when beneficial.'
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
),
lpim_check AS (
    SELECT
        'lock pages in memory' AS name,
        CASE 
            WHEN sql_memory_model_desc = 'LOCK_PAGES' THEN 'Enabled'
            ELSE 'Disabled'
        END AS current_value,
        'Recommended on dedicated SQL Server hosts' AS default_value,
        CASE 
            WHEN sql_memory_model_desc = 'LOCK_PAGES' THEN
                'LPIM is enabled. SQL Server memory is protected from being paged out by the OS, which helps memory stability.'
            ELSE
                'LPIM appears disabled. On dedicated servers, consider granting Lock Pages in Memory to the SQL Server service account to reduce paging risk.'
        END AS recommendation
    FROM sys.dm_os_sys_info
),
ifi_check AS (
    SELECT TOP (1)
        'instant file initialization' AS name,
        CASE 
            WHEN instant_file_initialization_enabled IN ('Y','y','1') THEN 'Enabled'
            WHEN instant_file_initialization_enabled IN ('N','n','0') THEN 'Disabled'
            ELSE CAST(instant_file_initialization_enabled AS NVARCHAR(50))
        END AS current_value,
        'Recommended for faster data-file growth and restores' AS default_value,
        CASE 
            WHEN instant_file_initialization_enabled IN ('Y','y','1') THEN
                'IFI is enabled. Data file growth and restore operations can complete much faster because zero-initialization is skipped for data files.'
            WHEN instant_file_initialization_enabled IN ('N','n','0') THEN
                'IFI is disabled. Consider enabling Perform Volume Maintenance Tasks for the SQL Server service account to speed up data file growth and restore operations. Log files are not affected.'
            ELSE
                'IFI status could not be cleanly determined from sys.dm_server_services. Verify service permissions manually if needed.'
        END AS recommendation
    FROM sys.dm_server_services
    WHERE servicename LIKE 'SQL Server (%'
       OR servicename LIKE 'SQL Server'
)
SELECT *
FROM base_config

UNION ALL

SELECT *
FROM lpim_check

UNION ALL

SELECT *
FROM ifi_check

ORDER BY name;
GO
/***********************************************************
 3) Database compatibility level (user databases)
***********************************************************/
PRINT '=== 3) Database compatibility level (user DBs) ===';

SELECT
    d.name                    AS database_name,
    d.compatibility_level,
    CASE d.compatibility_level
        WHEN 80  THEN 'SQL 2000'
        WHEN 90  THEN 'SQL 2005'
        WHEN 100 THEN 'SQL 2008/2008 R2'
        WHEN 110 THEN 'SQL 2012'
        WHEN 120 THEN 'SQL 2014'
        WHEN 130 THEN 'SQL 2016'
        WHEN 140 THEN 'SQL 2017'
        WHEN 150 THEN 'SQL 2019'
        WHEN 160 THEN 'SQL 2022'
        ELSE 'Unknown / future level'
    END AS compat_level_name,
    CASE 
        WHEN d.compatibility_level < 130 THEN
            'Compatibility level is older than SQL 2016 (130). Consider testing and raising compat level to leverage newer optimizer and features.'
        ELSE
            'Compatibility level is modern (>=130). Validate that it matches your target engine version and application support matrix.'
    END AS recommendation
FROM sys.databases d
WHERE d.database_id > 4              -- exclude system DBs
  AND d.name <> 'rdsadmin'
ORDER BY d.name;
GO

/***********************************************************
 4) Dedicated Admin Connection (DAC) configuration
***********************************************************/
PRINT '=== 4) Dedicated Admin Connection (DAC) configuration ===';

SELECT
    'remote admin connections'               AS setting_name,
    value_in_use                             AS value_in_use,
    CASE value_in_use
        WHEN 0 THEN 'Remote DAC disabled (only local DAC allowed).'
        WHEN 1 THEN 'Remote DAC enabled.'
    END AS status,
    CASE 
        WHEN value_in_use = 0 THEN
            'Recommendation: Enable remote DAC (sp_configure ''remote admin connections'', 1; RECONFIGURE) so you can connect in emergencies, especially on clustered or cloud-hosted servers.'
        ELSE
            'DAC is enabled for remote connections – ensure only DBAs know and use ADMIN: connections.'
    END AS recommendation
FROM sys.configurations
WHERE name = 'remote admin connections';
GO

/***********************************************************
 5) Backup and recovery posture (FULL/DIFF/LOG) – skipped on RDS
***********************************************************/
PRINT '=== 5) Backup and recovery posture (msdb backup history) ===';

DECLARE @is_rds BIT;

SELECT @is_rds =
    CASE 
        WHEN EXISTS (SELECT 1 FROM sys.databases WHERE name = 'rdsadmin') THEN 1
        ELSE 0
    END;

IF @is_rds = 1
BEGIN
    PRINT 'Backup posture section skipped (RDS detected – backups usually managed via RDS automated backups).';
END
ELSE
BEGIN
    ;WITH MostRecentBackups AS (
        SELECT
            database_name,
            MAX(CASE WHEN type = 'D' THEN backup_finish_date END) AS last_full,
            MAX(CASE WHEN type = 'I' THEN backup_finish_date END) AS last_diff,
            MAX(CASE WHEN type = 'L' THEN backup_finish_date END) AS last_log
        FROM msdb.dbo.backupset
        GROUP BY database_name
    )
    SELECT
        d.name                                  AS database_name,
        d.state_desc                            AS state_desc,
        d.recovery_model_desc                   AS recovery_model,
        b.last_full,
        DATEDIFF(DAY, b.last_full, GETDATE())   AS days_since_full,
        b.last_diff,
        DATEDIFF(DAY, b.last_diff, GETDATE())   AS days_since_diff,
        b.last_log,
        DATEDIFF(MINUTE, b.last_log, GETDATE()) AS minutes_since_log,
        CASE 
            WHEN b.last_full IS NULL THEN
                'No full backup found for this database – CRITICAL. Take a full backup as soon as possible.'
            WHEN DATEDIFF(DAY, b.last_full, GETDATE()) > 7 THEN
                'Last full backup is older than 7 days – WARNING. Consider more frequent fulls based on RPO.'
            ELSE
                'Full backup is within last 7 days – OK for many workloads; validate against your RPO/RTO.'
        END AS full_backup_recommendation,
        CASE 
            WHEN d.recovery_model_desc IN ('FULL','BULK_LOGGED') AND b.last_log IS NULL THEN
                'Database in FULL/BULK_LOGGED with no log backup – log will grow indefinitely. Configure regular log backups.'
            WHEN d.recovery_model_desc IN ('FULL','BULK_LOGGED') 
                 AND DATEDIFF(MINUTE, b.last_log, GETDATE()) > 30 THEN
                'Log backup overdue (>30 minutes) for FULL/BULK_LOGGED – risk of large log file and longer recovery.'
            ELSE
                'Log backup cadence appears reasonable for FULL/BULK_LOGGED (<=30 minutes) or DB is in SIMPLE.'
        END AS log_backup_recommendation
    FROM sys.databases d
    LEFT JOIN MostRecentBackups b ON d.name = b.database_name
    WHERE d.name <> 'tempdb'
      AND d.database_id > 4              -- skip system DBs
      AND d.name <> 'rdsadmin'           -- skip rdsadmin
    ORDER BY d.name;
END;
GO

/***********************************************************
 6) Database state and basic integrity-related settings
***********************************************************/
PRINT '=== 6) Database state and basic integrity-related settings ===';

SELECT
    d.name                         AS database_name,
    d.state_desc,
    d.user_access_desc,
    d.recovery_model_desc,
    d.page_verify_option_desc,
    d.is_auto_close_on,
    d.is_auto_shrink_on,
    CASE 
        WHEN d.state_desc <> 'ONLINE' THEN
            'Database is not ONLINE – investigate state and error log immediately.'
        WHEN d.page_verify_option_desc <> 'CHECKSUM' THEN
            'PAGE_VERIFY is not CHECKSUM – recommended to switch to CHECKSUM to detect IO corruption.'
        WHEN d.is_auto_close_on = 1 THEN
            'AUTO_CLOSE = ON – recommended to turn OFF for server workloads.'
        WHEN d.is_auto_shrink_on = 1 THEN
            'AUTO_SHRINK = ON – recommended to turn OFF to avoid fragmentation and IO spikes.'
        ELSE
            'Basic state and integrity-related settings look reasonable; confirm CHECKDB is part of regular maintenance.'
    END AS recommendation
FROM sys.databases d
WHERE d.database_id > 4   -- exclude system DBs
  AND d.name <> 'rdsadmin'
ORDER BY d.name;
GO

/***********************************************************
 7) File autogrowth recommendations (user databases only)
   Size tiers:
   - Small   < 512 MB       ->  512 MB growth
   - Medium  512 MB–4 GB    -> 1024 MB (1 GB) growth
   - Large   4–8 GB         -> 4096 MB (4 GB) growth
   - XLarge  > 8 GB         -> 8192 MB (8 GB) growth
***********************************************************/
PRINT '=== 7) File autogrowth settings and recommendations (user DBs only, excluding rdsadmin) ===';

;WITH files AS (
    SELECT
        d.name                      AS database_name,
        mf.name                     AS file_name,
        mf.type_desc,
        mf.physical_name,
        mf.size * 8 / 1024          AS size_mb,
        mf.is_percent_growth,
        mf.growth,                  -- pages (8 KB each)
        CASE 
            WHEN mf.size * 8 / 1024 < 512 THEN 'SMALL'                 -- < 512 MB
            WHEN mf.size * 8 / 1024 BETWEEN 512 AND 4096 THEN 'MEDIUM' -- 512 MB–4 GB
            WHEN mf.size * 8 / 1024 BETWEEN 4097 AND 8192 THEN 'LARGE' -- >4–8 GB
            ELSE 'XLARGE'                                              -- > 8 GB
        END AS size_category,
        CASE 
            WHEN mf.size * 8 / 1024 < 512 THEN 512      -- 512 MB
            WHEN mf.size * 8 / 1024 BETWEEN 512 AND 4096 THEN 1024   -- 1 GB
            WHEN mf.size * 8 / 1024 BETWEEN 4097 AND 8192 THEN 4096  -- 4 GB
            ELSE 8192                                               -- 8 GB
        END AS recommended_growth_mb,
        (mf.growth * 8 / 1024)      AS current_growth_mb
    FROM sys.master_files mf
    JOIN sys.databases   d ON d.database_id = mf.database_id
    WHERE d.database_id > 4               -- exclude master, tempdb, model, msdb
      AND d.name <> 'rdsadmin'            -- exclude RDS admin DB
)
SELECT
    database_name,
    file_name,
    type_desc,
    physical_name,
    size_mb,
    size_category,
    is_percent_growth,
    current_growth_mb,
    recommended_growth_mb,
    CASE 
        WHEN is_percent_growth = 1 THEN 
            'Autogrowth is percentage – change to fixed MB. Recommended fixed growth: ' 
            + CAST(recommended_growth_mb AS VARCHAR(20)) + ' MB.'
        WHEN is_percent_growth = 0 AND current_growth_mb = recommended_growth_mb THEN
            'Autogrowth is fixed and matches recommended growth of ' 
            + CAST(recommended_growth_mb AS VARCHAR(20)) + ' MB.'
        WHEN is_percent_growth = 0 AND current_growth_mb < recommended_growth_mb THEN
            'Autogrowth is fixed but smaller than recommended. Increase to ' 
            + CAST(recommended_growth_mb AS VARCHAR(20)) + ' MB to reduce number of growth events.'
        WHEN is_percent_growth = 0 AND current_growth_mb > recommended_growth_mb THEN
            'Autogrowth is fixed but larger than recommended. Consider reducing to ' 
            + CAST(recommended_growth_mb AS VARCHAR(20)) + ' MB and using manual pre‑growth for big changes.'
        ELSE
            'Review growth history and workload to fine‑tune autogrowth; use fixed MB and pre‑size where possible.'
    END AS autogrowth_recommendation
FROM files
ORDER BY database_name, type_desc, file_name;
GO

/***********************************************************
 8) Top 5 waits with detailed explanations
***********************************************************/
PRINT '=== 8) Top 5 wait stats (excluding benign waits) ===';

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
        wait_time_ms,
        signal_wait_time_ms,
        resource_wait_time_ms,
        CAST(wait_time_ms/1000.0 AS DECIMAL(18,1)) AS wait_seconds
    FROM waits
    WHERE wait_time_ms > 0
    ORDER BY wait_time_ms DESC
)
SELECT
    tw.wait_type,
    tw.waiting_tasks_count,
    tw.wait_seconds,
    tw.signal_wait_time_ms,
    tw.resource_wait_time_ms,
    CASE 
        WHEN tw.wait_type LIKE 'PAGEIOLATCH_%' THEN
            'IO waits reading data pages from disk into buffer pool. Focus: missing/inefficient indexes, large scans, slow/oversubscribed storage, and insufficient RAM for working set.'
        WHEN tw.wait_type LIKE 'READLOG' OR tw.wait_type LIKE 'WRITELOG' THEN
            'Transaction log IO bottleneck. Focus: place log on dedicated low‑latency volume, presize log to avoid frequent autogrowth, batch small transactions, and review synchronous AG/mirroring impact.'
        WHEN tw.wait_type IN ('LCK_M_S','LCK_M_U','LCK_M_X') THEN
            'Locking waits on shared/update/exclusive locks. Focus: identify blockers, shorten transactions, add/adjust indexes to avoid large scans, keep hot rows narrow, and evaluate RCSI where appropriate.'
        WHEN tw.wait_type IN ('CXPACKET','CXCONSUMER') THEN
            'Parallelism coordination waits. Focus: tune MAXDOP and cost threshold, fix plans doing big scans with skewed distribution, and ensure parallelism is aligned with workload type (OLTP vs reporting).'
        WHEN tw.wait_type = 'SOS_SCHEDULER_YIELD' THEN
            'CPU pressure. Focus: top CPU queries, eliminating scalar UDFs and RBAR patterns, updating stats, reviewing cardinality estimator, and adding CPU if code tuning is insufficient.'
        WHEN tw.wait_type LIKE 'ASYNC_NETWORK_IO' THEN
            'Client/network slow to consume rows. Focus: reduce result set sizes, avoid row‑by‑row consumption in app, add paging, and validate network throughput/latency.'
        WHEN tw.wait_type LIKE 'RESOURCE_SEMAPHORE%' THEN
            'Memory grant waits for large queries. Focus: tune big sorts/hash joins, add indexes to reduce rows processed, review MAXDOP (parallel plans request larger grants), and ensure max memory is set correctly.'
        ELSE
            'Less common top wait. Look up details in wait reference guides and correlate with CPU, IO, blocking, and memory metrics to design a targeted fix.'
    END AS explanation_recommendation
FROM top_waits AS tw
ORDER BY tw.wait_seconds DESC;
GO

/***********************************************************
 9) Top 10 resource-consuming queries (text + plan)
***********************************************************/
PRINT '=== 9) Top 10 resource-consuming queries by total logical reads ===';

;WITH qs AS (
    SELECT TOP (10)
        qs.plan_handle,
        qs.sql_handle,
        qs.statement_start_offset,
        qs.statement_end_offset,
        qs.total_logical_reads,
        qs.total_logical_writes,
        qs.total_worker_time,
        qs.total_elapsed_time,
        qs.execution_count,
        qs.total_logical_reads / NULLIF(qs.execution_count,0) AS avg_logical_reads,
        qs.total_worker_time   / NULLIF(qs.execution_count,0) AS avg_worker_time,
        qs.total_elapsed_time  / NULLIF(qs.execution_count,0) AS avg_elapsed_time
    FROM sys.dm_exec_query_stats AS qs
    ORDER BY qs.total_logical_reads DESC
)
SELECT
    DB_NAME(st.dbid)                              AS database_name,
    qs.execution_count,
    qs.total_logical_reads,
    qs.total_logical_writes,
    qs.total_worker_time,
    qs.total_elapsed_time,
    qs.avg_logical_reads,
    qs.avg_worker_time,
    qs.avg_elapsed_time,
    SUBSTRING(
        st.text,
        (qs.statement_start_offset/2) + 1,
        ((CASE 
            WHEN qs.statement_end_offset = -1 
                THEN LEN(CONVERT(NVARCHAR(MAX), st.text)) * 2
            ELSE qs.statement_end_offset 
          END - qs.statement_start_offset) / 2) + 1
    )                                            AS query_text,
    qp.query_plan,
    CASE 
        WHEN qs.avg_logical_reads > 50000 THEN 
            'Very high logical reads per execution – likely large scans or many lookups. Consider covering indexes and better filtering.'
        WHEN qs.avg_worker_time > 1000000 THEN 
            'CPU-heavy query. Review expressions, UDFs, and bad estimates; check join order and operators for inefficiencies.'
        WHEN qs.avg_elapsed_time > 2000 THEN 
            'Long runtime. Correlate with waits (IO vs CPU vs blocking) and check for tempdb spills and large sorts/hash joins.'
        ELSE 
            'High cumulative cost overall. Tune based on business priority; indexing and rewrites can usually lower resource use.'
    END AS recommendation
FROM qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)    AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
ORDER BY qs.total_logical_reads DESC;
GO

/***********************************************************
 10) High-cost queries with missing index suggestions
***********************************************************/
PRINT '=== 10) High-cost queries with missing index suggestions (use with caution) ===';

;WITH missing AS (
    SELECT 
        mid.database_id,
        mid.[object_id],
        mig.index_group_handle,
        mid.index_handle,
        (migs.user_seeks + migs.user_scans) AS user_reads,
        migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) AS improvement_measure,
        mid.equality_columns,
        mid.inequality_columns,
        mid.included_columns,
        mid.statement
    FROM sys.dm_db_missing_index_groups       AS mig
    JOIN sys.dm_db_missing_index_group_stats  AS migs ON mig.index_group_handle = migs.group_handle
    JOIN sys.dm_db_missing_index_details      AS mid  ON mig.index_handle     = mid.index_handle
)
SELECT TOP (20)
    DB_NAME(m.database_id)               AS database_name,
    m.statement                          AS table_statement,
    m.user_reads,
    CAST(m.improvement_measure AS DECIMAL(18,2)) AS improvement_measure,
    m.equality_columns,
    m.inequality_columns,
    m.included_columns,
    'CREATE NONCLUSTERED INDEX IX_Missing_' 
        + CONVERT(VARCHAR(10), m.index_handle)
        + ' ON ' + m.statement
        + ' (' + ISNULL(m.equality_columns, '')
        + CASE 
            WHEN m.equality_columns IS NOT NULL AND m.inequality_columns IS NOT NULL 
                THEN ',' + m.inequality_columns
            WHEN m.equality_columns IS NULL AND m.inequality_columns IS NOT NULL
                THEN m.inequality_columns
            ELSE '' 
          END + ')'
        + CASE 
            WHEN m.included_columns IS NOT NULL 
                THEN ' INCLUDE (' + m.included_columns + ')' 
            ELSE '' 
          END AS rough_index_statement,
    'High improvement_measure and user_reads suggest this index could reduce IO for important queries. ' +
    'Review against existing indexes, merge overlapping suggestions, and test on non‑production before creating. ' +
    'Do not blindly create every recommended index – they can overlap, bloat the index set, and slow writes.' AS recommendation
FROM missing AS m
WHERE m.improvement_measure IS NOT NULL
ORDER BY m.improvement_measure DESC;
GO
/***********************************************************
 11) Index usage – potential unused indexes (per database)
***********************************************************/
PRINT '=== 11) Index usage – potential unused indexes (for review, NOT auto-drop) ===';

DECLARE @db_name_usage sysname,
        @sql_usage nvarchar(max);

DECLARE db_usage_cursor CURSOR FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND name <> 'rdsadmin'
  AND state_desc = 'ONLINE';

OPEN db_usage_cursor;
FETCH NEXT FROM db_usage_cursor INTO @db_name_usage;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql_usage = N'
    USE ' + QUOTENAME(@db_name_usage) + N';
    PRINT ''--- Database: ' + @db_name_usage + ' ---'';

    SELECT TOP (50)
        DB_NAME() AS database_name,
        OBJECT_SCHEMA_NAME(s.object_id) AS schema_name,
        OBJECT_NAME(s.object_id) AS table_name,
        i.name AS index_name,
        s.user_seeks,
        s.user_scans,
        s.user_lookups,
        s.user_updates,
        CASE 
            WHEN s.user_seeks = 0 
             AND s.user_scans = 0 
             AND s.user_lookups = 0 
             AND s.user_updates > 0 THEN
                ''Index has only updates and no reads since last restart – candidate for further review as possible unused index.''
            ELSE
                ''Index shows at least some reads – confirm with workload knowledge and time range before considering changes.''
        END AS recommendation
    FROM sys.dm_db_index_usage_stats s
    JOIN sys.indexes i
      ON i.object_id = s.object_id
     AND i.index_id  = s.index_id
    WHERE s.database_id = DB_ID()
      AND OBJECTPROPERTY(s.object_id, ''IsUserTable'') = 1
      AND i.is_primary_key = 0
      AND i.is_unique = 0
    ORDER BY s.user_seeks + s.user_scans + s.user_lookups ASC,
             s.user_updates DESC;

    IF @@ROWCOUNT = 0
    BEGIN
        PRINT ''No qualifying index usage rows found in this database.'';
    END;
    ';

    EXEC (@sql_usage);

    FETCH NEXT FROM db_usage_cursor INTO @db_name_usage;
END

CLOSE db_usage_cursor;
DEALLOCATE db_usage_cursor;

PRINT '--- Note: sys.dm_db_index_usage_stats resets on restart; always correlate with uptime and workload before dropping indexes. ---';
GO
/***********************************************************
 12) Index fragmentation on large indexes (per database)
***********************************************************/
PRINT '=== 12) Index fragmentation on large indexes (per database) ===';

DECLARE @db_name sysname, @sql nvarchar(max);

DECLARE db_cursor CURSOR FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4          -- user DBs only
  AND name <> 'rdsadmin';

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
    USE ' + QUOTENAME(@db_name) + N';
    PRINT ''--- Database: ' + @db_name + ' ---'';

    ;WITH frag AS (
        SELECT
            DB_NAME()                         AS database_name,
            OBJECT_NAME(ips.object_id)        AS table_name,
            i.name                            AS index_name,
            ips.index_id,
            ips.avg_fragmentation_in_percent,
            ips.page_count
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''LIMITED'') AS ips
        JOIN sys.indexes i
          ON i.object_id = ips.object_id
         AND i.index_id  = ips.index_id
        WHERE ips.index_id > 0
          AND ips.page_count > 1000              -- only large indexes
          AND ips.avg_fragmentation_in_percent > 30 -- only highly fragmented
    )
    SELECT
        database_name,
        table_name,
        index_name,
        page_count,
        avg_fragmentation_in_percent,
        ''Consider REBUILD for this index (page_count > 1000 and fragmentation > 30%).'' AS recommendation
    FROM frag
    ORDER BY avg_fragmentation_in_percent DESC, page_count DESC;
    ';

    EXEC (@sql);

    FETCH NEXT FROM db_cursor INTO @db_name;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;
GO
