SELECT
    d.name AS driver,
    AVG(
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 11) AS REAL)
    ) AS avg_lap_seconds,
    s.event_name
FROM laps l
JOIN drivers d ON l.driver_id = d.id
JOIN sessions s ON l.session_id = s.id
WHERE s.session_name = 'Race' AND d.abbrevation = "VER" AND l.lap_time IS NOT NULL AND l.lap_time != ''
GROUP BY s.event_name
ORDER BY avg_lap_seconds;

SELECT
    d.name AS driver,
    AVG(
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 11) AS REAL)
    ) AS avg_lap_seconds_medium,
    s.event_name
FROM laps l
JOIN drivers d ON l.driver_id = d.id
JOIN sessions s ON l.session_id = s.id
WHERE s.session_name = 'Race'
  AND l.compound = 'MEDIUM'
  AND l.lap_time IS NOT NULL AND l.lap_time != ''
GROUP BY d.name
ORDER BY avg_lap_seconds_medium;

SELECT
    d.name AS driver,
    AVG(
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 11) AS REAL)
    ) AS avg_lap_seconds
FROM laps l
JOIN drivers d ON l.driver_id = d.id
JOIN sessions s ON l.session_id = s.id
WHERE s.session_name = 'Race'
  AND l.compound = 'MEDIUM'
  AND CAST(l.tyre_life AS INTEGER) >= 10
  AND l.lap_time IS NOT NULL AND l.lap_time != ''
GROUP BY d.name
ORDER BY avg_lap_seconds;

SELECT
    d.name AS driver,
    AVG(
        CAST(substr(l.sector1_time, INSTR(l.sector1_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.sector1_time, INSTR(l.sector1_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.sector1_time, INSTR(l.sector1_time, 'days ') + 11) AS REAL)
    ) AS avg_sector1,
    AVG(
        CAST(substr(l.sector2_time, INSTR(l.sector2_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.sector2_time, INSTR(l.sector2_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.sector2_time, INSTR(l.sector2_time, 'days ') + 11) AS REAL)
    ) AS avg_sector2,
    AVG(
        CAST(substr(l.sector3_time, INSTR(l.sector3_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.sector3_time, INSTR(l.sector3_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.sector3_time, INSTR(l.sector3_time, 'days ') + 11) AS REAL)
    ) AS avg_sector3
FROM laps l
JOIN drivers d ON l.driver_id = d.id
JOIN sessions s ON l.session_id = s.id
WHERE s.session_name = 'Race'
GROUP BY d.name;

SELECT
    d.name AS driver,
    AVG(r.grid_position - r.position) AS avg_positions_gained
FROM results r
JOIN drivers d ON r.driver_id = d.id
JOIN sessions s ON r.session_id = s.id
WHERE s.session_name = 'Race'
GROUP BY d.name
ORDER BY avg_positions_gained DESC;

SELECT
    d.name AS driver,
    l.lap_number,
    AVG(t.speed) AS avg_speed,
    AVG(t.throttle) AS avg_throttle,
    SUM(CASE WHEN t.brake = 1 THEN 1 ELSE 0 END) AS brake_events
FROM telemetry t
JOIN laps l ON t.lap_id = l.id
JOIN drivers d ON l.driver_id = d.id
WHERE d.abbrevation = "VER"
GROUP BY l.lap_number
ORDER BY l.lap_number;

SELECT
    d.name AS driver,
    w.rainfall,
    AVG(
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 8, 2) AS INTEGER) * 60 +
        CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 11) AS REAL)
    ) AS avg_lap_seconds
FROM laps l
JOIN drivers d ON l.driver_id = d.id
JOIN weather w ON l.session_id = w.session_id
JOIN sessions s ON l.session_id = s.id
WHERE s.session_name = 'Race' AND l.lap_time IS NOT NULL AND l.lap_time != ''
GROUP BY d.name, w.rainfall
ORDER BY d.name;



select AVG(
(x1...>         CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 5, 2) AS INTEGER) * 3600 +
(x1...>         CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 8, 2) AS INTEGER) * 60 +
(x1...>         CAST(substr(l.lap_time, INSTR(l.lap_time, 'days ') + 11) AS REAL)
(x1...>     ), d.abbrevation, s.event_name
   ...> from laps l
   ...> join drivers d on l.driver_id = d.id
   ...> join sessions s on l.session_id = s.id
   ...> where l.lap_time is not null and l.lap_time != ''
   ...> group by d.id, s.event_name;           