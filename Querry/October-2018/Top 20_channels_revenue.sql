INSERT INTO `pops-204909.monthly_reports.top_20_channels_revenue` 

(
    CMS_ID,
    DATE,
	CHANNEL_ID,
	CHANNEL_DISPLAY_NAME,
	REVENUE
)

SELECT t0.*

FROM

(

SELECT
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE
FROM 
(

--- MUSIC

SELECT 
      --t0.CMS,
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_music.p_content_owner_video_metadata_a1_yt_music`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID

--- ENTERTAINMENT

UNION ALL

SELECT 
      --t0.CMS,
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0  
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_entertainment.p_content_owner_video_metadata_a1_yt_entertainment`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID

--- AFFILIATE

UNION ALL

SELECT 
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  "AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_affiliate.p_content_owner_video_metadata_a1_yt_affiliate`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID           
            
--- KIDS

UNION ALL

SELECT 
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0                              
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_kids.p_content_owner_video_metadata_a1_yt_kids`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID           

--- MUSIC-TH

UNION ALL

SELECT 
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  --"KIDS" as `CMS`,
                  5 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0                             
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_th_music.p_content_owner_video_metadata_a1_yt_th_music`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID     

--- ENTERTAINMENT-TH

UNION ALL

SELECT 
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  --"KIDS" as `CMS`,
                  6 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0                     
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_th_entertainment.p_content_owner_video_metadata_a1_yt_th_entertainment`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID     

--- AFFILIATE-TH

UNION ALL

SELECT 
      t0.CMS_ID,
      t0.DATE,
      --t0.MONTH,
      --t0.QUARTER,
      --t0.YEAR,
      --t0.COUNTRY_CODE,
      t0.CHANNEL_ID,
      t0.CHANNEL_DISPLAY_NAME,
      t0.REVENUE,
      ROW_NUMBER() OVER (PARTITION BY t0.DATE ORDER BY t0.REVENUE DESC) as `RANK`
FROM
           (
           SELECT
                  --"KIDS" as `CMS`,
                  7 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  --extract(month from _PARTITIONTIME) as `MONTH`,
                  --extract(quarter from _PARTITIONTIME) as `QUARTER`,
                  --extract(year from _PARTITIONTIME) as `YEAR`,
                  --COUNTRY_CODE,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  --VIDEO_ID,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`   
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" 
            AND CHANNEL_ID is not null
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            ) t0                     
INNER JOIN (SELECT distinct channel_id FROM `pops-204909.yt_th_affiliate.p_content_owner_video_metadata_a1_yt_th_affiliate`) t1 on t0.CHANNEL_ID = t1.CHANNEL_ID                
) t0
--WHERE REVENUE > 0
) t0

LEFT JOIN (SELECT DISTINCT CMS_ID, DATE FROM `pops-204909.monthly_reports.top_20_channels_revenue`) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE