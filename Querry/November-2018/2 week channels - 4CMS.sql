SELECT
      CMS_ID,
      CHANNEL_ID,
      SUM(VIEWS) as VIEWS,
      IFNULL(SUM(REVENUE),0) AS REVENUE
FROM
(
 --MUSIC     
      SELECT 
            t0.CMS_ID,
            t0.CHANNEL_ID,
            t0.VIEWS,
            t1.REVENUE
      FROM
            (
            SELECT
                  1 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t0
      LEFT JOIN
            (
                  SELECT
                  1 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`

            ) t1 ON t0.DATE=t1.DATE and t0.CHANNEL_ID=t1.CHANNEL_ID AND t0.CMS_ID=t1.CMS_ID
UNION ALL

 --KIDS     
      SELECT 
            t0.CMS_ID,
            t0.CHANNEL_ID,
            t0.VIEWS,
            t1.REVENUE
      FROM
            (
            SELECT
                  2 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t0
      LEFT JOIN
            (
            SELECT
                  2 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t1 ON t0.DATE=t1.DATE and t0.CHANNEL_ID=t1.CHANNEL_ID AND t0.CMS_ID=t1.CMS_ID
UNION ALL

 --Entertainment     
      SELECT 
            t0.CMS_ID,
            t0.CHANNEL_ID,
            t0.VIEWS,
            t1.REVENUE
      FROM
            (
            SELECT
                  3 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t0
      LEFT JOIN
            (
            SELECT
                  3 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t1 ON t0.DATE=t1.DATE and t0.CHANNEL_ID=t1.CHANNEL_ID AND t0.CMS_ID=t1.CMS_ID
UNION ALL
 --AFFILIATE     
      SELECT 
            t0.CMS_ID,
            t0.CHANNEL_ID,
            t0.VIEWS,
            t1.REVENUE
      FROM
            (
            SELECT
                  4 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t0
      LEFT JOIN
            (
            SELECT
                  4 AS `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-09" AND "2018-11-15" AND uploader_type='self'
            GROUP BY `DATE`,  `CHANNEL_ID`
            ) t1 ON t0.DATE=t1.DATE and t0.CHANNEL_ID=t1.CHANNEL_ID AND t0.CMS_ID=t1.CMS_ID
)
GROUP BY CMS_ID,CHANNEL_ID