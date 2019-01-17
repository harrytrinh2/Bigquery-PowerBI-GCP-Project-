SELECT VIDEO_ID,SUM(VIEWS) AS `VIEWS`SELECT
      CHANNEL_ID,
      SUM(VIEWS) as VIEWS,
      IFNULL(SUM(REVENUE),0) AS REVENUE
FROM
(
      SELECT 
            t0.CHANNEL_ID,
            t1.VIEWS,
            t0.REVENUE
      FROM
            (
            SELECT
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-23" AND  "2018-11-29" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t0
      LEFT JOIN
            (
            SELECT
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-23" AND  "2018-11-29" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t1 ON t0.DATE=t1.DATE and t0.CHANNEL_ID=t1.CHANNEL_ID
)
GROUP BY CHANNEL_ID
FROM
    (
    SELECT
            video_id AS `VIDEO_ID`,
            SUM(views) AS `VIEWS`
    FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
    WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-23" AND  "2018-11-29" AND CHANNEL_ID = "UC7T_bNunNC25G3odVRs_-Eg"
    GROUP BY `DATE`, `VIDEO_ID`
    )
GROUP BY VIDEO_ID