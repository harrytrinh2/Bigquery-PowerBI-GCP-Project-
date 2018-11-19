--MUSIC
SELECT
        1 as `CMS_ID`,
        DATE(_PARTITIONTIME) as `DATE`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
WHERE DATE(_PARTITIONTIME) = "2018-10-30" 
GROUP BY `DATE`

UNION ALL 

--AFFILIATE
SELECT
        4 as `CMS_ID`,
        DATE(_PARTITIONTIME) as `DATE`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`   
WHERE DATE(_PARTITIONTIME) = "2018-10-30" 
GROUP BY `DATE`