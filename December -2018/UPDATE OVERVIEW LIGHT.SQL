INSERT INTO `pops-204909.monthly_reports.overview_light` 

(
    CMS_ID,
    DATE,
    VIEWS,
    REVENUE
)

--- KIDS

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
      (
      SELECT
            2 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
      WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
      GROUP BY `DATE`
      ) t0

--- ENTERTAINMENT

UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
      (
      SELECT
            3 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
      WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
      GROUP BY `DATE`
      ) t0
            
--- AFFILIATE

UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
      (
      SELECT
            4 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
      WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
      GROUP BY `DATE`
      ) t0
            
--- MUSIC-TH

UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
      (
      SELECT
            5 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`
      WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
      GROUP BY `DATE`
      ) t0
            
--- ENTERTAINMENT-TH

UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
      (
      SELECT
            6 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment`
      WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
      GROUP BY `DATE`
      ) t0
            
--- AFFILIATE TH

UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE
FROM
      (
      SELECT
            7 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`
      WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-11-30"
      GROUP BY `DATE`
      ) t0