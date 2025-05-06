-- System Queries: Check execution requests

SELECT *
FROM sys.dm_exec_requests
WHERE status = 'running'



-- Check top 5 long running queries: Created as a view in video

SELECT TOP 5 * 
FROM queryinsights.long_running_queries
ORDER BY last_run_total_elapsed_time_ms DESC

