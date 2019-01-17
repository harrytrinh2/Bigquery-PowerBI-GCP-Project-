INSERT INTO `pops-204909.monthly_reports.overview` 

(
    CMS_ID,
    DATE,
    LOCATION,
    EST_VIEWS,
    OWNED_VIEWS,
    EST_REVENUE,
    REVENUE,
    EST_RPM,
    RPM,
    SUBSCRIBERS_NET,
    EST_YOUTUBE_AD_REVENUE,
    AD_IMPRESSIONS,
    EST_MONETIZED_PLAYBACKS,
    AVERAGE_VIEW_DURATION,
    AVERAGE_VIEW_DURATION_MINUTE,
    AVERAGE_VIDEO_DURATION,
    WATCH_TIME_HOURS,
    TOTAL_VIDEO_DURATION,
    AVERAGE_PERCENTAGE_VIEWED
)

    --- AFFILIATE

    SELECT  t0.CMS_ID, 
            t0.DATE,
            t0.LOCATION, 
            t1.EST_VIEWS, t2.OWNED_VIEWS, 
            t0.EST_REVENUE, t2.REVENUE, 
            t0.EST_REVENUE/t1.EST_VIEWS*1000 as `EST_RPM`,
            t2.REVENUE/t2.OWNED_VIEWS*1000 as `RPM`,
            t1.SUBSCRIBERS_NET,
            t0.EST_YOUTUBE_AD_REVENUE,
            t0.AD_IMPRESSIONS,
            t0.EST_MONETIZED_PLAYBACKS,
            t1.AVERAGE_VIEW_DURATION, t1.AVERAGE_VIEW_DURATION/60 as `AVERAGE_VIEW_DURATION_MINUTE`,t2.AVERAGE_VIDEO_DURATION,
            t1.TOTAL_VIEW_DURATION/3600 as `WATCH_TIME_HOURS`, t2.TOTAL_VIDEO_DURATION,
            t1.AVERAGE_VIEW_DURATION/t2.AVERAGE_VIDEO_DURATION as `AVERAGE_PERCENTAGE_VIEWED`
    FROM
                (
                SELECT
                      4 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                      SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                      SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                      SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                      FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`   
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t0
    LEFT JOIN
                (
                SELECT
                      --"AFFILIATE" as `CMS`,
                      4 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(views) AS `EST_VIEWS`,
                      SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`,
                      AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                      SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                      FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`--,CHANNEL_ID,VIDEO_ID,`MONTH`,`QUARTER`,`YEAR`
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.LOCATION = t1.LOCATION 
    LEFT JOIN 
                (
                SELECT
                      4 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(owned_views) AS `OWNED_VIEWS`,
                      SUM(partner_revenue) as `REVENUE`,
                      SUM(video_duration_sec*owned_views) as `TOTAL_VIDEO_DURATION`,
                      AVG(video_duration_sec) as `AVERAGE_VIDEO_DURATION`
                      FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t2 on t0.CMS_ID = t2.CMS_ID and t0.DATE = t2.DATE and t0.LOCATION = t2.LOCATION 

    --- ENTERTAINMENT

    union all

    SELECT  t0.CMS_ID, 
            t0.DATE,
            t0.LOCATION, 
            t1.EST_VIEWS, t2.OWNED_VIEWS, 
            t0.EST_REVENUE, t2.REVENUE, 
            t0.EST_REVENUE/t1.EST_VIEWS*1000 as `EST_RPM`,
            t2.REVENUE/t2.OWNED_VIEWS*1000 as `RPM`,
            t1.SUBSCRIBERS_NET,
            t0.EST_YOUTUBE_AD_REVENUE,
            t0.AD_IMPRESSIONS,
            t0.EST_MONETIZED_PLAYBACKS,
            t1.AVERAGE_VIEW_DURATION, t1.AVERAGE_VIEW_DURATION/60 as `AVERAGE_VIEW_DURATION_MINUTE`,t2.AVERAGE_VIDEO_DURATION,
            t1.TOTAL_VIEW_DURATION/3600 as `WATCH_TIME_HOURS`, t2.TOTAL_VIDEO_DURATION,
            t1.AVERAGE_VIEW_DURATION/t2.AVERAGE_VIDEO_DURATION as `AVERAGE_PERCENTAGE_VIEWED`
    FROM
                (
                SELECT
                      3 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                      SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                      SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                      SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                      FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`    
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t0
    LEFT JOIN
                (
                SELECT
                      3 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(views) AS `EST_VIEWS`,
                      SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`,
                      AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                      SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                      FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`--,CHANNEL_ID,VIDEO_ID,`MONTH`,`QUARTER`,`YEAR`
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.LOCATION = t1.LOCATION 
    LEFT JOIN 
                (
                SELECT
                      --"ENTERTAINMENT" as `CMS`,
                      3 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,

                      SUM(owned_views) AS `OWNED_VIEWS`,
                      SUM(partner_revenue) as `REVENUE`,
                      SUM(video_duration_sec*owned_views) as `TOTAL_VIDEO_DURATION`,
                      AVG(video_duration_sec) as `AVERAGE_VIDEO_DURATION`
                      FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t2 on t0.CMS_ID = t2.CMS_ID and t0.DATE = t2.DATE and t0.LOCATION = t2.LOCATION 


    --- KIDS

    union all

    SELECT  t0.CMS_ID, 
            t0.DATE,
            t0.LOCATION, 
            t1.EST_VIEWS, t2.OWNED_VIEWS, 
            t0.EST_REVENUE, t2.REVENUE, 
            t0.EST_REVENUE/t1.EST_VIEWS*1000 as `EST_RPM`,
            t2.REVENUE/t2.OWNED_VIEWS*1000 as `RPM`,
            t1.SUBSCRIBERS_NET,
            t0.EST_YOUTUBE_AD_REVENUE,
            t0.AD_IMPRESSIONS,
            t0.EST_MONETIZED_PLAYBACKS,
            t1.AVERAGE_VIEW_DURATION, t1.AVERAGE_VIEW_DURATION/60 as `AVERAGE_VIEW_DURATION_MINUTE`,t2.AVERAGE_VIDEO_DURATION,
            t1.TOTAL_VIEW_DURATION/3600 as `WATCH_TIME_HOURS`, t2.TOTAL_VIDEO_DURATION,
            t1.AVERAGE_VIEW_DURATION/t2.AVERAGE_VIDEO_DURATION as `AVERAGE_PERCENTAGE_VIEWED`
    FROM
                (
                SELECT
                      --"KIDS" as `CMS`,
                      2 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      --CHANNEL_ID,
                      --VIDEO_ID,
                      SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                      SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                      SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                      SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                      FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t0
    LEFT JOIN
                (
                SELECT
                      --"KIDS" as `CMS`,
                      2 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(views) AS `EST_VIEWS`,
                      SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`,
                      AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                      SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                      FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.LOCATION = t1.LOCATION 
    LEFT JOIN 
                (
                SELECT
                      --"KIDS" as `CMS`,
                      2 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(owned_views) AS `OWNED_VIEWS`,
                      SUM(partner_revenue) as `REVENUE`,
                      SUM(video_duration_sec*owned_views) as `TOTAL_VIDEO_DURATION`,
                      AVG(video_duration_sec) as `AVERAGE_VIDEO_DURATION`
                      FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t2 on t0.CMS_ID = t2.CMS_ID and t0.DATE = t2.DATE and t0.LOCATION = t2.LOCATION 
  union all

  --- MUSIC
    SELECT  t0.CMS_ID, 
            t0.DATE,
            t0.LOCATION, 
            t1.EST_VIEWS, t2.OWNED_VIEWS, 
            t0.EST_REVENUE, t2.REVENUE, 
            t0.EST_REVENUE/t1.EST_VIEWS*1000 as `EST_RPM`,
            t2.REVENUE/t2.OWNED_VIEWS*1000 as `RPM`,
            t1.SUBSCRIBERS_NET,
            t0.EST_YOUTUBE_AD_REVENUE,
            t0.AD_IMPRESSIONS,
            t0.EST_MONETIZED_PLAYBACKS,
            t1.AVERAGE_VIEW_DURATION, t1.AVERAGE_VIEW_DURATION/60 as `AVERAGE_VIEW_DURATION_MINUTE`,t2.AVERAGE_VIDEO_DURATION,
            t1.TOTAL_VIEW_DURATION/3600 as `WATCH_TIME_HOURS`, t2.TOTAL_VIDEO_DURATION,
            t1.AVERAGE_VIEW_DURATION/t2.AVERAGE_VIDEO_DURATION as `AVERAGE_PERCENTAGE_VIEWED`
    FROM
                (
                SELECT
                      --"MUSIC" as `CMS`,
                      1 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                      SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                      SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                      SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
                      FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"

                GROUP BY `LOCATION`,`DATE`
                ) t0
    LEFT JOIN
                (
                SELECT
                      --"MUSIC" as `CMS`,
                      1 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(views) AS `EST_VIEWS`,
                      SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`,
                      AVG(average_view_duration_seconds) as `AVERAGE_VIEW_DURATION`,
                      SUM(watch_time_minutes*60) as `TOTAL_VIEW_DURATION`
                      FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.LOCATION = t1.LOCATION 
    LEFT JOIN 
                (
                SELECT
                      --"MUSIC" as `CMS`,
                      1 as `CMS_ID`,
                      TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                      CASE
                        WHEN country_code = "VN"
                        THEN "LOCAL"
                        ELSE "OVERSEAS"
                      END AS `LOCATION`,
                      SUM(owned_views) AS `OWNED_VIEWS`,
                      SUM(partner_revenue) as `REVENUE`,
                      SUM(video_duration_sec*owned_views) as `TOTAL_VIDEO_DURATION`,
                      AVG(video_duration_sec) as `AVERAGE_VIDEO_DURATION`
                      FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
                WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
                GROUP BY `LOCATION`,`DATE`
                ) t2 on t0.CMS_ID = t2.CMS_ID and t0.DATE = t2.DATE and t0.LOCATION = t2.LOCATION