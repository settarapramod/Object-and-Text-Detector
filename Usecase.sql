WITH Schedule AS (
    SELECT 
        CAST('2024-10-16 07:30:00' AS DATETIME) AS start_time, 
        CAST('2024-10-16 14:00:00' AS DATETIME) AS end_time,
        CAST('2024-10-16 09:30:00' AS DATETIME) AS break1_start, 
        CAST('2024-10-16 09:45:00' AS DATETIME) AS break1_end,
        CAST('2024-10-16 11:45:00' AS DATETIME) AS break2_start, 
        CAST('2024-10-16 12:15:00' AS DATETIME) AS break2_end
),
AgentLogins AS (
    -- Example: Multiple login/logout sessions
    SELECT 
        CAST('2024-10-16 07:45:00' AS DATETIME) AS sign_in_time, 
        CAST('2024-10-16 09:35:00' AS DATETIME) AS sign_out_time
    UNION ALL
    SELECT 
        CAST('2024-10-16 09:50:00' AS DATETIME), 
        CAST('2024-10-16 12:00:00' AS DATETIME)
    UNION ALL
    SELECT 
        CAST('2024-10-16 12:20:00' AS DATETIME), 
        CAST('2024-10-16 13:45:00' AS DATETIME)
),
AdjustedLogins AS (
    -- Adjust login sessions to fit within the working schedule
    SELECT 
        CASE 
            WHEN sign_in_time > s.start_time THEN sign_in_time 
            ELSE s.start_time 
        END AS actual_start,
        CASE 
            WHEN sign_out_time < s.end_time THEN sign_out_time 
            ELSE s.end_time 
        END AS actual_end,
        al.sign_in_time, al.sign_out_time
    FROM 
        AgentLogins al
    CROSS JOIN 
        Schedule s
    WHERE 
        sign_out_time > s.start_time AND sign_in_time < s.end_time
),
Breaks AS (
    -- Extract breaks that overlap with any login session
    SELECT 
        break_start, break_end
    FROM (
        SELECT break1_start AS break_start, break1_end AS break_end FROM Schedule
        UNION ALL
        SELECT break2_start, break2_end FROM Schedule
    ) b
    JOIN AdjustedLogins al 
      ON b.break_end > al.actual_start AND b.break_start < al.actual_end
),
BreakMinutes AS (
    -- Calculate total break minutes overlapping with working periods
    SELECT 
        SUM(DATEDIFF(MINUTE, 
            CASE 
                WHEN b.break_start > al.actual_start THEN b.break_start 
                ELSE al.actual_start 
            END, 
            CASE 
                WHEN b.break_end < al.actual_end THEN b.break_end 
                ELSE al.actual_end 
            END
        )) AS total_break_minutes
    FROM Breaks b
    JOIN AdjustedLogins al 
      ON b.break_end > al.actual_start AND b.break_start < al.actual_end
)
-- Final query to compute both compliance minutes and total worked minutes
SELECT 
    -- Compliance minutes (within schedule, excluding breaks)
    SUM(DATEDIFF(MINUTE, actual_start, actual_end)) 
    - COALESCE((SELECT total_break_minutes FROM BreakMinutes), 0) AS compliance_minutes,

    -- Total minutes worked (regardless of schedule)
    SUM(DATEDIFF(MINUTE, sign_in_time, sign_out_time)) AS total_worked_minutes
FROM 
    AdjustedLogins;
