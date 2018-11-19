INSERT INTO `pops-204909.monthly_reports.overview_light` 

(
    CMS_ID,
    DATE,
    VIEWS,
    REVENUE,
    VIEWS_ANALYTICS
)

--- MUSIC

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            1 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            1 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE
--- KIDS

UNION ALL

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            2 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            2 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE

--- ENTERTAINMENT

UNION ALL

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            3 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            3 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE

--- AFFILIATE

UNION ALL

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            4 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            4 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE

--- MUSIC-TH

UNION ALL

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            5 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            5 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_th_music.p_content_owner_basic_a3_yt_th_music`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE

--- ENTERTAINMENT-TH

UNION ALL

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            6 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment` 
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            6 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE
--- AFFILIATE TH

UNION ALL

SELECT t0.CMS_ID, 
        t0.DATE,
        t0.OWNED_VIEWS,
        t0.REVENUE,
        t1.VIEWS_ANALYTICS
FROM
      (
      SELECT
            7 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(owned_views) AS `OWNED_VIEWS`,
            SUM(partner_revenue) as `REVENUE`
            FROM  `pops-204909.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`
      WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t0
LEFT JOIN
      (
      SELECT
            7 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            SUM(views) AS `VIEWS_ANALYTICS`
      FROM `pops-204909.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate`
      WHERE  DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
      GROUP BY `DATE`
      ) t1  on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE