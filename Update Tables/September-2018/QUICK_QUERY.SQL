SELECT
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        SUM(partner_revenue) AS `REVENUE`
FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31" 
GROUP BY `DATE`