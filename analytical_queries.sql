-- 1. Daily average value per category over the last 30 days
SELECT 
    category, 
    DATE(timestamp) AS event_date, 
    AVG(value) AS daily_avg_value
FROM events
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY category, event_date
ORDER BY event_date DESC, category;

-- 2. Number of events inside each predefined region over the last 30 days
-- Uses a spatial join (ST_Intersects)
SELECT 
    r.name AS region_name, 
    COUNT(e.id) AS event_count
FROM regions r
JOIN events e ON ST_Intersects(r.geom, e.geom)
WHERE e.timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY r.name
ORDER BY event_count DESC;

-- 3. Top 5 "hotspots" (grid cells) in the last 7 days
-- Groups points into roughly ~1km cells (0.01 degree) using ST_SnapToGrid
SELECT 
    ST_AsText(ST_SnapToGrid(geom, 0.01)) AS grid_cell,
    SUM(value) AS total_value
FROM events
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY grid_cell
ORDER BY total_value DESC
LIMIT 5;
