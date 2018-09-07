--- MUSIC

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

--- KIDS

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

--- ENTERTAINMENT

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

--- AFFILIATE

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

--- MUSIC-TH

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "MUSIC-TH" as `CMS`,
                  5 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_th_music.p_content_owner_basic_a3_yt_th_music`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "MUSIC-TH" as `CMS`,
                  5 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_th_music.p_content_owner_estimated_revenue_a1_yt_th_music`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

--- ENTERTAINMENT-TH

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "ENTERTAINMENT-TH" as `CMS`,
                  6 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "ENTERTAINMENT-TH" as `CMS`,
                  6 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_estimated_revenue_a1_yt_th_entertainment`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

--- AFFILIATE-TH

UNION ALL

SELECT  t1.CMS_ID, t1.CMS, 
        t2.DATEVALUE as `DATE`, t2.DATEKEY, cast(t2.WEEK_REPORT as int64) as `WEEK`, t2.YEAR,
        t1.LOCATION,
        t1.EST_VIEWS,
        t1.WATCH_TIME/60 as `WATCH_TIME`,
        t0.EST_REVENUE,
        t0.AD_IMPRESSIONS,
        t0.EST_YOUTUBE_AD_REVENUE,
        t1.VIDEOS_ADDED,
        t1.SUBSCRIBERS_NET
FROM   
            (
            SELECT
                  "AFFILIATE-TH" as `CMS`,
                  7 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`,
                  SUM(watch_time_minutes) as `WATCH_TIME`,
                  SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
                  SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
            FROM `929791903032.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `DATE`,`LOCATION`
            ) t1             

LEFT JOIN
            
            (
            SELECT
                  "AFFILIATE-TH" as `CMS`,
                  7 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_estimated_revenue_a1_yt_th_affiliate`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.CMS = t0.CMS and t1.LOCATION = t0.LOCATION and t1.DATE = t0.DATE 
            
RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018