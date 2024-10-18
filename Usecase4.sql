-- Step 1: CTE to Adjust Agent Logins within Shift Boundaries
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
        AND S.SegmentType = 'C'  -- Shift segments only
        AND SI.TimeOut >= S.StartDate 
        AND SI.TimeIn <= S.EndDate
),

-- Step 2: Calculate Total Compliance Minutes per Agent per Day
ComplianceMinutes AS (
    SELECT 
        AgentId,
        WorkDate,
        SUM(DATEDIFF(MINUTE, AdjustedTimeIn, AdjustedTimeOut)) AS TotalComplianceMinutes
    FROM ShiftBoundaries
    GROUP BY AgentId, WorkDate
),

-- Step 3: Calculate Total Break Overlaps (minutes worked during breaks)
BreakOverlaps AS (
    SELECT 
        SB.AgentId,
        SB.WorkDate,
        SUM(
            DATEDIFF(MINUTE,
                CASE WHEN SB.AdjustedTimeIn < B.StartDate THEN B.StartDate ELSE SB.AdjustedTimeIn END,
                CASE WHEN SB.AdjustedTimeOut > B.EndDate THEN B.EndDate ELSE SB.AdjustedTimeOut END
            )
        ) AS TotalBreakOverlapMinutes
    FROM ShiftBoundaries SB
    JOIN Schedule B 
        ON SB.AgentId = B.AgentId 
        AND B.SegmentType = 'B'  -- Break segments only
        AND SB.AdjustedTimeOut > B.StartDate 
        AND SB.AdjustedTimeIn < B.EndDate
    GROUP BY SB.AgentId, SB.WorkDate
),

-- Step 4: Calculate Total Scheduled Minutes and Break Minutes per Agent per Day
ScheduledBreaks AS (
    SELECT 
        S.AgentId,
        CAST(S.StartDate AS DATE) AS WorkDate,
        SUM(DATEDIFF(MINUTE, S.StartDate, S.EndDate)) AS TotalScheduledMinutes,
        SUM(CASE WHEN S.SegmentType = 'B' 
                 THEN DATEDIFF(MINUTE, S.StartDate, S.EndDate) 
                 ELSE 0 
            END) AS TotalBreakMinutes
    FROM Schedule S
    GROUP BY S.AgentId, CAST(S.StartDate AS DATE)
)

-- Step 5: Final Query to Aggregate Results to One Record per Agent per Day
SELECT 
    C.AgentId,
    C.WorkDate,
    C.TotalComplianceMinutes AS ComplianceMinutes,
    C.TotalComplianceMinutes - ISNULL(B.TotalBreakOverlapMinutes, 0) AS AdherenceMinutes,
    S.TotalScheduledMinutes - S.TotalBreakMinutes AS ActualScheduledMinutes
FROM ComplianceMinutes C
JOIN ScheduledBreaks S 
    ON C.AgentId = S.AgentId AND C.WorkDate = S.WorkDate
LEFT JOIN BreakOverlaps B 
    ON C.AgentId = B.AgentId AND C.WorkDate = B.WorkDate;
