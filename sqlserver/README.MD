# SQL Server Instance Health Snapshot

A read-only SQL Server health-check script that collects key configuration, safety, backup, performance, and index-health information from an instance.

## What it checks

This script currently includes:

1. Instance version and last restart
2. Key configuration:
   - Max server memory
   - MAXDOP
   - Cost threshold for parallelism
   - Lock Pages in Memory (LPIM)
   - Instant File Initialization (IFI)
3. Database compatibility level for user databases
4. Dedicated Admin Connection (DAC) configuration
5. Backup and recovery posture (skipped on RDS)
6. Database state and integrity-related settings
7. File autogrowth recommendations for user databases
8. Top wait statistics
9. Top resource-consuming queries
10. Missing index recommendations
11. Potential unused index review
12. Fragmentation review for large indexes

## Purpose

The goal of this script is to provide a quick operational snapshot of a SQL Server instance and highlight areas that may need DBA review.

It is intended for:

- SQL Server health reviews
- Quick diagnostics
- Environment baselining
- Operational handoff notes
- DBA internal tooling

## Requirements

- SQL Server with permission to query DMVs and catalog views used by the script
- Recommended: `VIEW SERVER STATE`
- SSMS or any compatible SQL query tool

Some sections may require elevated access depending on the environment and SQL Server version.

## Usage

1. Open the script in SSMS.
2. Review the header and disclaimer.
3. Run it against the target SQL Server instance.
4. Review the output section by section.
5. Validate all recommendations before making changes.

## Notes

- The script is read-only.
- It excludes `rdsadmin` where appropriate.
- Backup posture is skipped automatically when RDS is detected.
- Index usage stats reset after SQL Server restart.
- Fragmentation checks are limited to larger indexes based on thresholds defined in the script.

## Output style

The script is designed to return practical output with:
- current setting or metric
- contextual interpretation
- recommendation text

It is meant to be easy to review directly in SSMS result grids.

## Safety

This script does **not** automatically:
- change configuration
- create indexes
- rebuild indexes
- alter files
- modify databases

Any recommendations must be manually reviewed and tested.

## Disclaimer

This script is provided **AS IS** with no warranty or guarantee of fitness for any purpose.

Use it at your own risk. I do not accept responsibility for downtime, performance issues, configuration mistakes, data loss, or any other impact caused directly or indirectly by using this script or acting on its recommendations.

## File

- `sqlserver_health_snapshot_v1.0.sql`





## Author

Sam Ebeid
