-- name: total_records
SELECT COUNT(*) AS total_records
FROM service_requests;

-- name: distinct_unique_keys
SELECT COUNT(DISTINCT unique_key) AS distinct_unique_keys
FROM service_requests;

-- name: duplicate_keys
SELECT unique_key, COUNT(*) AS times_stored
FROM service_requests
GROUP BY unique_key
HAVING COUNT(*) > 1;

-- name: missing_required_values
SELECT
    SUM(CASE WHEN created_date   IS NULL THEN 1 ELSE 0 END) AS missing_created_date,
    SUM(CASE WHEN complaint_type IS NULL THEN 1 ELSE 0 END) AS missing_complaint_type,
    SUM(CASE WHEN borough        IS NULL THEN 1 ELSE 0 END) AS missing_borough,
    SUM(CASE WHEN status         IS NULL THEN 1 ELSE 0 END) AS missing_status
FROM service_requests;

-- name: records_by_borough
SELECT borough, COUNT(*) AS requests
FROM service_requests
GROUP BY borough
ORDER BY requests DESC;

-- name: records_by_status
SELECT status, COUNT(*) AS requests
FROM service_requests
GROUP BY status
ORDER BY requests DESC;

-- name: records_by_day
SELECT created_date_only AS day, COUNT(*) AS requests
FROM service_requests
GROUP BY created_date_only
ORDER BY day;

-- name: top_complaint_types
SELECT complaint_type, COUNT(*) AS requests
FROM service_requests
GROUP BY complaint_type
ORDER BY requests DESC
LIMIT 5;

-- name: resolution_by_borough
SELECT borough,
       COUNT(resolution_hours)          AS closed_with_time,
       ROUND(AVG(resolution_hours), 2)  AS avg_resolution_hours,
       ROUND(MIN(resolution_hours), 2)  AS min_hours,
       ROUND(MAX(resolution_hours), 2)  AS max_hours
FROM service_requests
WHERE resolution_hours IS NOT NULL
GROUP BY borough
ORDER BY avg_resolution_hours;

-- name: sample_rows
SELECT unique_key, created_date, complaint_type, borough, status,
       ROUND(resolution_hours, 2) AS resolution_hours
FROM service_requests
ORDER BY unique_key
LIMIT 5;
