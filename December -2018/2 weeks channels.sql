
SELECT
      CHANNEL_ID,
      SUM(VIEWS) as VIEWS,
      IFNULL(SUM(REVENUE),0) AS REVENUE
FROM
(
      SELECT 
            t0.CHANNEL_ID,
            t0.VIEWS,
            t1.REVENUE
      FROM
            ( 
            SELECT
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-12-14" and "2018-12-20"
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t0
      LEFT JOIN
            (
            SELECT
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-12-14" and "2018-12-20"
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t1 ON t0.DATE=t1.DATE and t0.CHANNEL_ID=t1.CHANNEL_ID
)
GROUP BY CHANNEL_ID