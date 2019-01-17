INSERT INTO `pops-204909.KIDS.Doraemon` 

(
  CMS_ID ,
  DATE,
  CHANNEL_ID,
  VIDEO_ID,
  EST_VIEWS,
  EST_REVENUE
)
SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-08-01" and "2017-08-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-08-01" and "2017-08-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID

UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-09-01" and "2017-09-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-09-01" and "2017-09-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-10-01" and "2017-10-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-10-01" and "2017-10-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-11-01" and "2017-11-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-11-01" and "2017-11-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-12-01" and "2017-12-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-12-01" and "2017-12-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between  "2018-05-01" and "2018-05-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between  "2018-06-01" and "2018-06-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between  "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
UNION ALL

SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between  "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0
LEFT JOIN
            (
            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID