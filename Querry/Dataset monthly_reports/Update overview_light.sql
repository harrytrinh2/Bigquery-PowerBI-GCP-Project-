INSERT INTO `pops-204909.monthly_reports.overview_light` 

(
    CMS_ID,
    DATE,
    VIEWS,
    REVENUE
)

--- MUSIC
SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"ENTERTAINMENT" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0

--- KIDS

union all

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0
            
--- ENTERTAINMENT

union all

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0
            
--- AFFILIATE

union all

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0
            
--- MUSIC-TH

union all

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"MUSIC-TH" as `CMS`,
                  5 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0
            
--- ENTERTAINMENT-TH

union all

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"ENTERTAINMENT-TH" as `CMS`,
                  6 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0
            
--- AFFILIATE

union all

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
            (
            SELECT
                  --"AFFILIATE-TH" as `CMS`,
                  7 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
                  FROM `929791903032.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`
            ) t0            