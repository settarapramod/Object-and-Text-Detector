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

-- Step 2: Calculate Total Break Overlaps for the Day
DailyBreakOverlaps AS (
    SELECT 
        SB.AgentId,
        SB.WorkDate,
        SUM(
            DATEDIFF(MINUTE,
                CASE WHEN SB.AdjustedTimeIn < B.StartDate THEN B.StartDate ELSE SB.AdjustedTimeIn END,
                CASE WHEN SB.AdjustedTimeOut > B.EndDate THEN B.EndDate ELSE SB.AdjustedTimeOut END
            )
        ) AS TotalBreakMinutes
    FROM ShiftBoundaries SB
    JOIN Schedule B 
        ON SB.AgentId = B.AgentId 
        AND B.SegmentType = 'B'  -- Break segments only
        AND SB.AdjustedTimeOut > B.StartDate 
        AND SB.AdjustedTimeIn < B.EndDate
    GROUP BY SB.AgentId, SB.WorkDate
),

-- Step 3: Calculate Compliance Minutes per Agent per Day
DailyCompliance AS (
    SELECT 
        AgentId,
        WorkDate,
        SUM(DATEDIFF(MINUTE, AdjustedTimeIn, AdjustedTimeOut)) AS TotalComplianceMinutes
    FROM ShiftBoundaries
    GROUP BY AgentId, WorkDate
),

-- Step 4: Calculate Total Scheduled and Actual Work Minutes per Agent per Day
DailyScheduled AS (
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

-- Step 5: Final Query to Aggregate and Calculate Adherence, Compliance, and Actual Scheduled Minutes
SELECT 
    DC.AgentId,
    DC.WorkDate,
    DC.TotalComplianceMinutes AS ComplianceMinutes,
    DC.TotalComplianceMinutes - ISNULL(DBO.TotalBreakMinutes, 0) AS AdherenceMinutes,
    DS.TotalScheduledMinutes - DS.TotalBreakMinutes AS ActualScheduledMinutes
FROM DailyCompliance DC
JOIN DailyScheduled DS 
    ON DC.AgentId = DS.AgentId AND DC.WorkDate = DS.WorkDate
LEFT JOIN DailyBreakOverlaps DBO 
    ON DC.AgentId = DBO.AgentId AND DC.WorkDate = DBO.WorkDate;
