SELECT
      VIDEO_ID,
      SUM(VIEWS) as VIEWS,
      IFNULL(SUM(REVENUE),0) AS REVENUE
FROM
(
      SELECT 
            t0.VIDEO_ID,
            t1.VIEWS,
            t0.REVENUE
      FROM
            (
            SELECT
                  DATE(_PARTITIONTIME) as `DATE`,
                  video_id as `VIDEO_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-02" AND "2018-11-08" AND CHANNEL_ID='UCS9JKqDiIJqps1S8kpcx2Xg'
            GROUP BY `DATE`,  `VIDEO_ID`
            ) t0
      LEFT JOIN
            (
            SELECT
                  DATE(_PARTITIONTIME) as `DATE`,
                  video_id as `VIDEO_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-02" AND "2018-11-08" AND CHANNEL_ID='UCS9JKqDiIJqps1S8kpcx2Xg'
            GROUP BY `DATE`,  `VIDEO_ID`
            ) t1 ON t0.DATE=t1.DATE and t0.VIDEO_ID=t1.VIDEO_ID
)
GROUP BY VIDEO_ID