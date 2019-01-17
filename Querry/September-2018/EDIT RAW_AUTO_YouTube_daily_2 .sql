--- MUSIC

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, 
        cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.CHANNEL_ID,
        CASE WHEN CAST(SUBSTR(t1.CHANNEL_NAME_REPORT,1,2) AS int64 ) >=10 THEN t1.CHANNEL_NAME_REPORT ELSE CONCAT('0',t1.CHANNEL_NAME_REPORT) END AS `CHANNEL_NAME_REPORT`,
        SUM(t1.EST_VIEWS) as `EST_VIEWS`,
        SUM(t0.EST_REVENUE) as EST_REVENUE,
        SUM(t1.SUBSCRIBERS_NET) as `SUBSCRIBERS_NET`
FROM   
            (
            SELECT
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
				          t1.CHANNEL_ID,
                  t2.CHANNEL_NAME_REPORT,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music` t1
			      INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "MUSIC"
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT
            ) t1 

LEFT JOIN
            
            (
            SELECT
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  t1.CHANNEL_ID,
                  t2.CHANNEL_NAME_REPORT,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music` t1
            INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "MUSIC"
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.DATE = t0.DATE and t1.CHANNEL_ID = t0.CHANNEL_ID          
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE 
WHERE t2.YEAR = 2018
GROUP BY  t1.CMS_ID, t1.CMS, `DATE`, `WEEK`, t2.YEAR, t1.CHANNEL_ID, t1.CHANNEL_NAME_REPORT

--- KIDS

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, 
        cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.CHANNEL_ID,
        CASE WHEN CAST(SUBSTR(t1.CHANNEL_NAME_REPORT,1,2) AS int64 ) >=10 THEN t1.CHANNEL_NAME_REPORT ELSE CONCAT('0',t1.CHANNEL_NAME_REPORT) END AS `CHANNEL_NAME_REPORT`,
        SUM(t1.EST_VIEWS) as `EST_VIEWS`,
        SUM(t0.EST_REVENUE) as EST_REVENUE,
        SUM(t1.SUBSCRIBERS_NET) as `SUBSCRIBERS_NET`
FROM   
            (
            SELECT
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
				          t1.CHANNEL_ID,
                  t2.CHANNEL_NAME_REPORT,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids` t1
			      INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "KIDS"
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT
            ) t1 

LEFT JOIN
            
            (
            SELECT
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  t1.CHANNEL_ID,
                  t2.CHANNEL_NAME_REPORT,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids` t1
            INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "KIDS"
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.DATE = t0.DATE and t1.CHANNEL_ID = t0.CHANNEL_ID        
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE 
WHERE t2.YEAR = 2018
GROUP BY  t1.CMS_ID, t1.CMS, `DATE`, `WEEK`, t2.YEAR, t1.CHANNEL_ID, t1.CHANNEL_NAME_REPORT

--- ENTERTAINMENT

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, 
        cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.CHANNEL_ID,
        CASE WHEN CAST(SUBSTR(t1.CHANNEL_NAME_REPORT,1,2) AS int64 ) >=10 THEN t1.CHANNEL_NAME_REPORT ELSE CONCAT('0',t1.CHANNEL_NAME_REPORT) END AS `CHANNEL_NAME_REPORT`,
        SUM(t1.EST_VIEWS) as `EST_VIEWS`,
        SUM(t0.EST_REVENUE) as EST_REVENUE,
        SUM(t1.SUBSCRIBERS_NET) as `SUBSCRIBERS_NET`
FROM   
            (
            SELECT
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
				          t1.CHANNEL_ID,
                  t2.CHANNEL_NAME_REPORT,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment` t1
			      INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "ENTERTAINMENT"
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT
            ) t1 

LEFT JOIN
            
            (
            SELECT
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  t1.CHANNEL_ID,
                  t2.CHANNEL_NAME_REPORT,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment` t1
            INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "ENTERTAINMENT"
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.DATE = t0.DATE and t1.CHANNEL_ID = t0.CHANNEL_ID      
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE 
WHERE t2.YEAR = 2018
GROUP BY  t1.CMS_ID, t1.CMS, `DATE`, `WEEK`, t2.YEAR, t1.CHANNEL_ID, t1.CHANNEL_NAME_REPORT