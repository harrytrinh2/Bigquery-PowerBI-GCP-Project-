SELECT  
        1 AS `CMS_ID`,
         t0.CHANNEL_ID,
         t1.CHANNEL_DISPLAY_NAME
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,

FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids` AS t0
INNER JOIN `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids` AS t1    ON t0.CHANNEL_ID = t1.CHANNEL_ID
WHERE    DATE(_PARTITIONTIME) between "2017-08-01" and "2017-08-31"


----SELECT CHANNEL_ID => CHANNEL_DISPLAY_NAME
SELECT 
    DISTINCT t0.CHANNEL_ID,
    t1.CHANNEL_DISPLAY_NAME
FROM
    (
    SELECT
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            CHANNEL_ID
    FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`   
    WHERE DATE(_PARTITIONTIME) between "2017-08-01" and "2017-08-31"
    ) t0
LEFT JOIN
    (
    SELECT
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            CHANNEL_ID,
            CHANNEL_DISPLAY_NAME
    FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
    WHERE DATE(_PARTITIONTIME) between "2017-08-01" and "2017-08-31"
    ) t1 on  t0.DATE = t1.DATE and t0.CHANNEL_ID= t1.CHANNEL_ID