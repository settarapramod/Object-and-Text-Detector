-- Step 1: CTE to Adjust Sign-in/Sign-out Times Within Shift Boundaries
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
        AND S.SegmentType = 'C'  -- Only consider shift segments
        AND SI.TimeOut >= S.StartDate 
        AND SI.TimeIn <= S.EndDate
),

-- Step 2: Calculate Overlapping Break Minutes
BreakOverlaps AS (
    SELECT 
        SB.AgentId,
        SB.WorkDate,
        SUM(
            DATEDIFF(MINUTE,
                CASE WHEN SB.AdjustedTimeIn < B.StartDate THEN B.StartDate ELSE SB.AdjustedTimeIn END,
                CASE WHEN SB.AdjustedTimeOut > B.EndDate THEN B.EndDate ELSE SB.AdjustedTimeOut END
            )
        ) AS BreakOverlapMinutes
    FROM ShiftBoundaries SB
    JOIN Schedule B 
        ON SB.AgentId = B.AgentId 
        AND B.SegmentType = 'B'  -- Only consider break segments
        AND SB.AdjustedTimeOut > B.StartDate 
        AND SB.AdjustedTimeIn < B.EndDate
    GROUP BY SB.AgentId, SB.WorkDate
),

-- Step 3: Calculate Total Compliance Minutes Per Agent Per Day
Compliance AS (
    SELECT 
        AgentId,
        WorkDate,
        SUM(DATEDIFF(MINUTE, AdjustedTimeIn, AdjustedTimeOut)) AS ComplianceMinutes
    FROM ShiftBoundaries
    GROUP BY AgentId, WorkDate
)

-- Step 4: Final Query to Calculate Adherence, Compliance, and Scheduled Minutes
SELECT 
    C.AgentId,
    C.WorkDate,
    C.ComplianceMinutes,
    DATEDIFF(MINUTE, S.StartDate, S.EndDate) AS ScheduledMinutes,
    C.ComplianceMinutes - ISNULL(B.BreakOverlapMinutes, 0) AS AdherenceMinutes
FROM Compliance C
JOIN Schedule S 
    ON C.AgentId = S.AgentId 
    AND S.SegmentType = 'C' 
    AND C.WorkDate = CAST(S.StartDate AS DATE)
LEFT JOIN BreakOverlaps B 
    ON C.AgentId = B.AgentId 
    AND C.WorkDate = B.WorkDate;
