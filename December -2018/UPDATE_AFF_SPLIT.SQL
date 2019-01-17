--AFFILIATE
SELECT 
    DATE,
    BU,
    SUM(EST_VIEWS) AS `EST_VIEWS`,
    SUM(EST_REVENUE) AS `EST_REVENUE`
FROM
(
    SELECT  t1.DATE,
            t1.EST_VIEWS,
            t0.EST_REVENUE,
            t0.CHANNEL_ID,
            IFNULL(t2.BU, 'Others') AS `BU`
    FROM
                (
                SELECT
                    DATE(_PARTITIONTIME) as `DATE`,
                    SUM(views) AS `EST_VIEWS`,
                    channel_id as `CHANNEL_ID`
                FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
                WHERE DATE(_PARTITIONTIME) >= "2018-10-01"
                GROUP BY `DATE`, `CHANNEL_ID`
                ) t1
    LEFT JOIN
                (
                SELECT
                    DATE(_PARTITIONTIME) as `DATE`,
                    channel_id as `CHANNEL_ID`,
                    SUM(estimated_partner_revenue) AS `EST_REVENUE`
                FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`   
                WHERE DATE(_PARTITIONTIME) >= "2018-10-01"
                GROUP BY `DATE`,`CHANNEL_ID`
                ) t0 on t1.DATE = t0.DATE and t1.CHANNEL_ID = t0.CHANNEL_ID
    LEFT JOIN
                (
                SELECT
                    BU,
                    CHANNEL_ID
                FROM `pops-204909.CMS_AFFILIATE.AFF_Channels_Split`
                GROUP BY `CHANNEL_ID`,`BU`
                ) t2 on t1.CHANNEL_ID = t2.CHANNEL_ID
)
GROUP BY `BU`,`DATE`