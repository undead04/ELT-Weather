-- Test logic: suitability_score and advice_text consistency
-- 1. Nếu suitability_score >= 80, advice_text phải là 'Thời tiết tuyệt vời, thoải mái hoạt động!'
-- 2. Nếu suitability_score >= 50 và < 80, advice_text phải là 'Nên hạn chế hoạt động mạnh.'
-- 3. Nếu suitability_score < 50, advice_text phải là 'Độc hại, không ra ngoài!'

SELECT *
FROM {{ ref('dm_activity_plan') }}
WHERE 
    (suitability_score >= 80 AND advice_text != 'Thời tiết tuyệt vời, thoải mái hoạt động!')
    OR (suitability_score >= 50 AND suitability_score < 80 AND advice_text != 'Nên hạn chế hoạt động mạnh.')
    OR (suitability_score < 50 AND advice_text != 'Độc hại, không ra ngoài!')
