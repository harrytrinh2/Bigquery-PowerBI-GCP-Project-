-- DELETE THANG 9 ESTIMATED
DELETE `pops-204909.KIDS.POPS_Kids_Basic` WHERE DATE='2018-09-01 00:00:00 UTC'
--INSERT RAW THANG 9
INSERT INTO `pops-204909.KIDS.POPS_Kids_Basic` 
(
  CMS_ID ,
  DATE,
  CHANNEL_ID,
  VIDEO_ID,
  VIEWS,
  REVENUE
)
SELECT   
        2 as `CMS_ID`,
        DATE,
        CHANNEL_ID,
        VIDEO_ID,
        OWNED_VIEWS as VIEWS,
        REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            )

--INSERT ESTIMATED THANG 10
INSERT INTO `pops-204909.KIDS.POPS_Kids_Basic` 
(
  CMS_ID ,
  DATE,
  CHANNEL_ID,
  VIDEO_ID,
  VIEWS,
  REVENUE
)
SELECT   
        2 as `CMS_ID`,
        t1.DATE,
        t1.CHANNEL_ID,
        t1.VIDEO_ID,
        t2.EST_VIEWS as VIEWS,
        t1.EST_REVENUE as REVENUE
FROM
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31"  AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID
