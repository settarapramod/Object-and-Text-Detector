WITH Compliance AS (
    SELECT 
        AgentId,
        SUM(DATEDIFF(MINUTE, 
            CASE WHEN TimeIn < (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeIn END,
            CASE WHEN TimeOut > (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeOut END
        )) AS ComplianceMinutes
    FROM SignInOut
    WHERE TimeOut >= (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C')
      AND TimeIn <= (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C')
    GROUP BY AgentId
)
SELECT * FROM Compliance;
WITH Adherence AS (
    SELECT 
        AgentId,
        SUM(DATEDIFF(MINUTE, 
            CASE WHEN TimeIn < (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeIn END,
            CASE WHEN TimeOut > (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeOut END
        )) - (
            SELECT ISNULL(SUM(DATEDIFF(MINUTE, StartDate, EndDate)), 0)
            FROM Schedule
            WHERE SegmentType = 'B'
        ) AS AdherenceMinutes
    FROM SignInOut
    WHERE TimeOut >= (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C')
      AND TimeIn <= (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C')
    GROUP BY AgentId
)
SELECT * FROM Adherence;

WITH Scheduled AS (
    SELECT 
        AgentId,
        DATEDIFF(MINUTE, 
            MIN(StartDate), MAX(EndDate)) - (
            SELECT ISNULL(SUM(DATEDIFF(MINUTE, StartDate, EndDate)), 0)
            FROM Schedule
            WHERE SegmentType = 'B'
        ) AS ScheduledMinutes
    FROM Schedule
    WHERE SegmentType = 'C'
    GROUP BY AgentId
)
SELECT * FROM Scheduled;

WITH Compliance AS (
    SELECT 
        AgentId,
        SUM(DATEDIFF(MINUTE, 
            CASE WHEN TimeIn < (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeIn END,
            CASE WHEN TimeOut > (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeOut END
        )) AS ComplianceMinutes
    FROM SignInOut
    GROUP BY AgentId
),
Adherence AS (
    SELECT 
        AgentId,
        SUM(DATEDIFF(MINUTE, 
            CASE WHEN TimeIn < (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MIN(StartDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeIn END,
            CASE WHEN TimeOut > (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') 
                 THEN (SELECT MAX(EndDate) FROM Schedule WHERE SegmentType = 'C') ELSE TimeOut END
        )) - (
            SELECT ISNULL(SUM(DATEDIFF(MINUTE, StartDate, EndDate)), 0)
            FROM Schedule
            WHERE SegmentType = 'B'
        ) AS AdherenceMinutes
    FROM SignInOut
    GROUP BY AgentId
),
Scheduled AS (
    SELECT 
        AgentId,
        DATEDIFF(MINUTE, MIN(StartDate), MAX(EndDate)) - (
            SELECT ISNULL(SUM(DATEDIFF(MINUTE, StartDate, EndDate)), 0)
            FROM Schedule
            WHERE SegmentType = 'B'
        ) AS ScheduledMinutes
    FROM Schedule
    WHERE SegmentType = 'C'
    GROUP BY AgentId
)
SELECT 
    C.AgentId,
    C.ComplianceMinutes,
    A.AdherenceMinutes,
    S.ScheduledMinutes
FROM Compliance C
JOIN Adherence A ON C.AgentId = A.AgentId
JOIN Scheduled S ON C.AgentId = S.AgentId;
