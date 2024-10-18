-- Step 1: CTE to calculate all compliance minutes from sign-in and sign-out records
WITH Compliance AS (
    SELECT 
        SI.AgentId,
        CAST(SI.TimeIn AS DATE) AS DayDate,
        SUM(DATEDIFF(MINUTE, SI.TimeIn, SI.TimeOut)) AS TotalComplianceMinutes
    FROM SignInOut SI
    GROUP BY SI.AgentId, CAST(SI.TimeIn AS DATE)
),

-- Step 2: CTE to calculate all scheduled minutes and break minutes
Scheduled AS (
    SELECT 
        S.AgentId,
        CAST(S.StartDate AS DATE) AS DayDate,
        SUM(DATEDIFF(MINUTE, S.StartDate, S.EndDate)) AS TotalScheduledMinutes,
        SUM(CASE WHEN S.SegmentType = 'B' THEN DATEDIFF(MINUTE, S.StartDate, S.EndDate) ELSE 0 END) AS TotalBreakMinutes
    FROM Schedule S
    GROUP BY S.AgentId, CAST(S.StartDate AS DATE)
),

-- Step 3: CTE to calculate the overlap of sign-in/out times with breaks
BreaksOverlap AS (
    SELECT 
        SI.AgentId,
        CAST(SI.TimeIn AS DATE) AS DayDate,
        SUM(
            DATEDIFF(MINUTE,
                CASE 
                    WHEN SI.TimeIn < B.StartDate THEN B.StartDate 
                    ELSE SI.TimeIn 
                END,
                CASE 
                    WHEN SI.TimeOut > B.EndDate THEN B.EndDate 
                    ELSE SI.TimeOut 
                END
            )
        ) AS TotalBreakOverlapMinutes
    FROM SignInOut SI
    JOIN Schedule B ON SI.AgentId = B.AgentId 
        AND B.SegmentType = 'B'
        AND SI.TimeOut >= B.StartDate 
        AND SI.TimeIn <= B.EndDate
    GROUP BY SI.AgentId, CAST(SI.TimeIn AS DATE)
)

-- Step 4: Final select to aggregate everything to one record per agent per day
SELECT 
    C.AgentId,
    C.DayDate,
    ISNULL(C.TotalComplianceMinutes, 0) AS ComplianceMinutes,
    ISNULL(C.TotalComplianceMinutes, 0) - ISNULL(BO.TotalBreakOverlapMinutes, 0) AS AdherenceMinutes,
    ISNULL(S.TotalScheduledMinutes, 0) - ISNULL(S.TotalBreakMinutes, 0) AS ActualScheduledMinutes
FROM Compliance C
FULL OUTER JOIN Scheduled S 
    ON C.AgentId = S.AgentId AND C.DayDate = S.DayDate
FULL OUTER JOIN BreaksOverlap BO 
    ON C.AgentId = BO.AgentId AND C.DayDate = BO.DayDate
ORDER BY C.AgentId, C.DayDate;
