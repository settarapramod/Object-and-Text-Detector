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
    -- Example of multiple login and logout sessions for the agent
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
    -- Adjust each login session to fit within the scheduled work hours
    SELECT 
        GREATEST(sign_in_time, s.start_time) AS actual_start,
        LEAST(sign_out_time, s.end_time) AS actual_end
    FROM 
        AgentLogins al, Schedule s
    WHERE 
        sign_out_time > s.start_time AND sign_in_time < s.end_time
),
Breaks AS (
    -- Extract the breaks that overlap with any login sessions
    SELECT 
        break_start, break_end
    FROM (
        SELECT 
            break1_start AS break_start, break1_end AS break_end 
        FROM Schedule
        UNION ALL
        SELECT 
            break2_start, break2_end 
        FROM Schedule
    ) b
    JOIN AdjustedLogins al 
      ON b.break_end > al.actual_start AND b.break_start < al.actual_end
),
BreakMinutes AS (
    -- Calculate total break minutes overlapping with working periods
    SELECT 
        SUM(DATEDIFF(MINUTE, 
            GREATEST(b.break_start, al.actual_start), 
            LEAST(b.break_end, al.actual_end)
        )) AS total_break_minutes
    FROM Breaks b
    JOIN AdjustedLogins al 
      ON b.break_end > al.actual_start AND b.break_start < al.actual_end
)
-- Final query to calculate total working minutes excluding breaks
SELECT 
    SUM(DATEDIFF(MINUTE, actual_start, actual_end)) - 
    COALESCE((SELECT total_break_minutes FROM BreakMinutes), 0) AS total_working_minutes
FROM 
    AdjustedLogins;
