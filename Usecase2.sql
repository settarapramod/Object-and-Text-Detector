-- Step 1: Create CTE to Adjust Sign-in and Sign-out Times Within Shift Boundaries
WITH ShiftBoundaries AS (
    SELECT 
        SI.AgentId,
        CAST(SI.TimeIn AS DATE) AS WorkDate,
        CASE 
            WHEN SI.TimeIn < S.StartDate THEN S.StartDate 
            ELSE SI.TimeIn 
        END AS AdjustedTimeIn,
        CASE 
            WHEN SI.TimeOut > S.EndDate THEN S.EndDate 
            ELSE SI.TimeOut 
        END AS AdjustedTimeOut
    FROM SignInOut SI
    JOIN Schedule S 
        ON SI.AgentId = S.AgentId 
        AND S.SegmentType = 'C'  -- Only consider shift (C) segments
        AND SI.TimeOut >= S.StartDate 
        AND SI.TimeIn <= S.EndDate
),

-- Step 2: Calculate Total Compliance Minutes Per Agent Per Day
Compliance AS (
    SELECT 
        AgentId,
        WorkDate,
        SUM(DATEDIFF(MINUTE, AdjustedTimeIn, AdjustedTimeOut)) AS ComplianceMinutes
    FROM ShiftBoundaries
    GROUP BY AgentId, WorkDate
),

-- Step 3: Calculate Total Break Minutes Per Agent Per Day
Breaks AS (
    SELECT 
        AgentId,
        CAST(StartDate AS DATE) AS WorkDate,
        SUM(DATEDIFF(MINUTE, StartDate, EndDate)) AS TotalBreakMinutes
    FROM Schedule
    WHERE SegmentType = 'B'  -- Only consider break (B) segments
    GROUP BY AgentId, CAST(StartDate AS DATE)
)

-- Step 4: Final Query to Calculate Adherence and Scheduled Minutes
SELECT 
    C.AgentId,
    C.WorkDate,
    C.ComplianceMinutes,
    DATEDIFF(MINUTE, S.StartDate, S.EndDate) - ISNULL(B.TotalBreakMinutes, 0) AS ScheduledMinutes,
    C.ComplianceMinutes - ISNULL(B.TotalBreakMinutes, 0) AS AdherenceMinutes
FROM Compliance C
JOIN Schedule S 
    ON C.AgentId = S.AgentId 
    AND S.SegmentType = 'C' 
    AND C.WorkDate = CAST(S.StartDate AS DATE)
LEFT JOIN Breaks B 
    ON C.AgentId = B.AgentId 
    AND C.WorkDate = B.WorkDate;
