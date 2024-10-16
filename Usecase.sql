WITH Schedule AS (
    SELECT 
        '2024-10-16 07:30:00' AS start_time, 
        '2024-10-16 14:00:00' AS end_time,
        '2024-10-16 09:30:00' AS break1_start, 
        '2024-10-16 09:45:00' AS break1_end,
        '2024-10-16 11:45:00' AS break2_start, 
        '2024-10-16 12:15:00' AS break2_end
),
AgentSignInOut AS (
    SELECT 
        '2024-10-16 07:45:00' AS sign_in_time, 
        '2024-10-16 13:45:00' AS sign_out_time
)
SELECT 
    GREATEST(sign_in_time, start_time) AS actual_start,
    LEAST(sign_out_time, end_time) AS actual_end,
    -- Calculate total working time excluding breaks
    (
        DATEDIFF(MINUTE, GREATEST(sign_in_time, start_time), LEAST(sign_out_time, end_time))
        -- Subtract first break if applicable
        - CASE 
            WHEN break1_start < LEAST(sign_out_time, end_time) 
                 AND break1_end > GREATEST(sign_in_time, start_time) 
            THEN DATEDIFF(MINUTE, 
                GREATEST(break1_start, GREATEST(sign_in_time, start_time)), 
                LEAST(break1_end, LEAST(sign_out_time, end_time))
            )
            ELSE 0
          END
        -- Subtract second break if applicable
        - CASE 
            WHEN break2_start < LEAST(sign_out_time, end_time) 
                 AND break2_end > GREATEST(sign_in_time, start_time) 
            THEN DATEDIFF(MINUTE, 
                GREATEST(break2_start, GREATEST(sign_in_time, start_time)), 
                LEAST(break2_end, LEAST(sign_out_time, end_time))
            )
            ELSE 0
          END
    ) AS total_working_minutes
FROM 
    Schedule, AgentSignInOut;
