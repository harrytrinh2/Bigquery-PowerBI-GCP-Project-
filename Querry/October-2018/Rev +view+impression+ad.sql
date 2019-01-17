--MUSIC
SELECT   
        1 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION

UNION ALL

--KIDS

SELECT   
        2 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 

UNION ALL

--ENTERTAINMENT

SELECT   
        3 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 

UNION ALL

--Affiliate

SELECT   
        4 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
UNION ALL

--music -TH

SELECT   
        5 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_music.p_content_owner_estimated_revenue_a1_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 

UNION ALL

-- ENTERTAINMENT_TH
SELECT   
        6 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_estimated_revenue_a1_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
UNION ALL
-- Affiliate-TH
SELECT   
        7 as `CMS_ID`,
        t0.LOCATION, 
        t0.DATE,
        t0.OWNED_VIEWS as VIEWS,
        t0.REVENUE as REVENUE,
        t1.AD_IMPRESSIONS as AD_IMPRESSIONS,
        t1.EST_YOUTUBE_AD_REVENUE as EST_YOUTUBE_AD_REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(ad_impressions) AS `AD_IMPRESSIONS`,
                  SUM(estimated_youtube_ad_revenue) AS `EST_YOUTUBE_AD_REVENUE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_estimated_revenue_a1_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 