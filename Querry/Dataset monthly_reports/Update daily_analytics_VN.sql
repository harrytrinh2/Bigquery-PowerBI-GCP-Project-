INSERT INTO `pops-204909.monthly_reports.daily_analytics_VN` 

(
  CMS_ID,
  DATE,
	CHANNEL_ID,
	CHANNEL_DISPLAY_NAME,
	VIDEO_ID,
	VIDEO_TITLE,
	EST_VIEWS,
	EST_REVENUE
)

SELECT t0.* FROM

(
--- AFFILIATE

SELECT  t0.CMS_ID, 
        t0.DATE, --t0.MONTH, t0.QUARTER, t0.YEAR, 
        --t0.LOCATION, 
        t0.CHANNEL_ID, t2.CHANNEL_DISPLAY_NAME,
        t0.VIDEO_ID, t2.VIDEO_TITLE,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  --"AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,                
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
                  --SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  --SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`   
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            ) t0
INNER JOIN
            (
            SELECT
                  "AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
                  --AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                  --SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                  FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --and VIDEO_ID IN ("4e7iDjQrUB8")
            --AND COUNTRY_CODE = "VN"
            --and 
            --views <> 0
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            HAVING SUM(views) >= 1000
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID --and t0.MONTH = t1.MONTH and t0.QUARTER = t1.QUARTER and t0.YEAR = t1.YEAR 

LEFT JOIN `929791903032.dimension.d_videos` t2 on t0.VIDEO_ID = t2.VIDEO_ID and t0.CMS_ID = t2.CMS_ID
            
--- ENTERTAINMENT

union all

SELECT  t0.CMS_ID, 
        t0.DATE, --t0.MONTH, t0.QUARTER, t0.YEAR, 
        --t0.LOCATION, 
        t0.CHANNEL_ID, t2.CHANNEL_DISPLAY_NAME,
        t0.VIDEO_ID, t2.VIDEO_TITLE,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  --"entertainment" as `CMS`,
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
                  --SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  --SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                  FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            ) t0
INNER JOIN
            (
            SELECT
                  "entertainment" as `CMS`,
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
                  --AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                  --SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                  FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --and VIDEO_ID IN ("4e7iDjQrUB8")
            --AND COUNTRY_CODE = "VN"
            --and 
            --views <> 0
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            HAVING SUM(views) >= 1000
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID --and t0.MONTH = t1.MONTH and t0.QUARTER = t1.QUARTER and t0.YEAR = t1.YEAR 
          
LEFT JOIN `929791903032.dimension.d_videos` t2 on t0.VIDEO_ID = t2.VIDEO_ID and t0.CMS_ID = t2.CMS_ID
          
--- MUSIC

union all

SELECT  t0.CMS_ID, 
        t0.DATE, --t0.MONTH, t0.QUARTER, t0.YEAR, 
        --t0.LOCATION, 
        t0.CHANNEL_ID, t2.CHANNEL_DISPLAY_NAME,
        t0.VIDEO_ID, t2.VIDEO_TITLE,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  --"music" as `CMS`,
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
                  --SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  --SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                  FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            ) t0
INNER JOIN
            (
            SELECT
                  "music" as `CMS`,
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
                  --AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                  --SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                  FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --and VIDEO_ID IN ("4e7iDjQrUB8")
            --AND COUNTRY_CODE = "VN"
            --and 
            --views <> 0
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            HAVING SUM(views) >= 1000
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID --and t0.MONTH = t1.MONTH and t0.QUARTER = t1.QUARTER and t0.YEAR = t1.YEAR 
          
LEFT JOIN `929791903032.dimension.d_videos` t2 on t0.VIDEO_ID = t2.VIDEO_ID and t0.CMS_ID = t2.CMS_ID
          
 --- KIDS

union all

SELECT  t0.CMS_ID, 
        t0.DATE, --t0.MONTH, t0.QUARTER, t0.YEAR, 
        --t0.LOCATION, 
        t0.CHANNEL_ID, t2.CHANNEL_DISPLAY_NAME,
        t0.VIDEO_ID, t2.VIDEO_TITLE,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  --"kids" as `CMS`,
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
                  --SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  --SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                  FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            ) t0
INNER JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,  
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
                  --AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                  --SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                  FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) >= "2018-07-01"
            --and VIDEO_ID IN ("4e7iDjQrUB8")
            --AND COUNTRY_CODE = "VN"
            --and 
            --views <> 0
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID--,`MONTH`,`QUARTER`,`YEAR`
            HAVING SUM(views) >= 1000
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID --and t0.MONTH = t1.MONTH and t0.QUARTER = t1.QUARTER and t0.YEAR = t1.YEAR 

LEFT JOIN `929791903032.dimension.d_videos` t2 on t0.VIDEO_ID = t2.VIDEO_ID and t0.CMS_ID = t2.CMS_ID
) t0
LEFT JOIN (select distinct CMS_ID, DATE from `pops-204909.monthly_reports.daily_analytics_VN`) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE
WHERE t1.DATE is null