WITH AgentSchedule AS (
    SELECT 
        AgentId, 
        CAST(StartDate AS DATE) AS WorkDay, 
        StartDate AS shift_start, 
        EndDate AS shift_end,
        DATEDIFF(MINUTE, StartDate, EndDate) AS shift_minutes
    FROM Schedule
    WHERE segmenttype = 'C'  -- Shift times only
),
Breaks AS (
    SELECT 
        AgentId, 
        StartDate AS break_start, 
        EndDate AS break_end
    FROM Schedule
    WHERE segmenttype = 'B'  -- Break times only
),
LoginSessions AS (
    SELECT 
        agentid, 
        CAST(timen AS DATE) AS WorkDay, 
        timen AS login_time, 
        timeout AS logout_time
    FROM AgentLogins
),
AdjustedLogins AS (
    -- Adjust login sessions to fit within the shift time
    SELECT 
        ls.agentid,
        ls.WorkDay,
        CASE 
            WHEN ls.login_time > s.shift_start THEN ls.login_time 
            ELSE s.shift_start 
        END AS actual_login,
        CASE 
            WHEN ls.logout_time < s.shift_end THEN ls.logout_time 
            ELSE s.shift_end 
        END AS actual_logout
    FROM 
        LoginSessions ls
    JOIN AgentSchedule s 
        ON ls.agentid = s.AgentId AND ls.WorkDay = s.WorkDay
    WHERE 
        ls.logout_time > s.shift_start AND ls.login_time < s.shift_end
),
BreakMinutes AS (
    -- Calculate break minutes overlapping with login sessions
    SELECT 
        al.agentid,
        al.WorkDay,
        SUM(DATEDIFF(MINUTE, 
            CASE 
                WHEN b.break_start > al.actual_login THEN b.break_start 
                ELSE al.actual_login 
            END, 
            CASE 
                WHEN b.break_end < al.actual_logout THEN b.break_end 
                ELSE al.actual_logout 
            END
        )) AS total_break_minutes
    FROM 
        Breaks b
    JOIN AdjustedLogins al 
        ON b.AgentId = al.agentid
        AND b.break_end > al.actual_login 
        AND b.break_start < al.actual_logout
    GROUP BY al.agentid, al.WorkDay
)
-- Final query to compute compliance minutes, total worked minutes, and shift minutes
SELECT 
    s.AgentId,
    s.WorkDay,
    s.shift_minutes,  -- Total shift minutes
    SUM(DATEDIFF(MINUTE, al.actual_login, al.actual_logout)) 
        - COALESCE(bm.total_break_minutes, 0) AS compliance_minutes,  -- Compliance minutes (shift minus breaks)
    SUM(DATEDIFF(MINUTE, ls.login_time, ls.logout_time)) AS total_worked_minutes  -- Total minutes worked, regardless of shift
FROM 
    AgentSchedule s
JOIN 
    AdjustedLogins al ON s.AgentId = al.agentid AND s.WorkDay = al.WorkDay
JOIN 
    LoginSessions ls ON al.agentid = ls.agentid AND al.WorkDay = ls.WorkDay
LEFT JOIN 
    BreakMinutes bm ON al.agentid = bm.agentid AND al.WorkDay = bm.WorkDay
GROUP BY 
    s.AgentId, s.WorkDay, s.shift_minutes;
