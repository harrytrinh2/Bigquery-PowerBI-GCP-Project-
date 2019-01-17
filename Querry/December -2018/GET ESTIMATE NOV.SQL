--MUSIC
SELECT
        1 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 

UNION ALL

--KIDS
SELECT
        2 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 

UNION ALL

--ENTERTAINMENT
SELECT
        3 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 

UNION ALL

--AFFILIATE
SELECT
        4 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 

UNION ALL

--MUSIC-TH
SELECT
        5 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_th_music.p_content_owner_estimated_revenue_a1_yt_th_music`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 

UNION ALL

--ENT-TH
SELECT
        6 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_th_entertainment.p_content_owner_estimated_revenue_a1_yt_th_entertainment`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 

UNION ALL

--AFF-TH
SELECT
        7 as `CMS_ID`,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
FROM `pops-204909.yt_th_affiliate.p_content_owner_estimated_revenue_a1_yt_th_affiliate`   
WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" 