INSERT INTO `pops-204909.monthly_reports.kids_detail` 

(
    CMS_ID,
    DATE,
    CHANNEL_ID,
    VIDEO_ID,
    EST_VIEWS,
    EST_REVENUE
)

--- KIDS
SELECT t0.*

FROM

(
SELECT  t0.CMS_ID, 
        t0.DATE, 
        t0.CHANNEL_ID, t0.VIDEO_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM
            (
            SELECT
                  --"kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-01-01" and "2018-08-31"
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
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2017-01-01" and "2018-08-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
) t0

LEFT JOIN (select distinct CMS_ID, DATE FROM `929791903032.monthly_reports.kids_detail`) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE

WHERE t1.DATE is null
