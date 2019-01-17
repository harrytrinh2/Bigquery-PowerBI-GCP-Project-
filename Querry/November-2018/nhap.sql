SELECT
      POSSESSION,
      SUM(VIEWS) AS `VIEWS`,
      SUM(REVENUE) AS `REVENUE`
FROM
(
    SELECT
        1 as `CMS_ID`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        video_id AS `VIDEO_ID`,
        IF
            ( VIDEO_ID IN (SELECT DISTINCT(VIDEO_ID) FROM `pops-204909.Working.2017_video_id`), "OLD",
            (IF  (VIDEO_ID IN (SELECT DISTINCT(video_id) 
                            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
                            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31"  ),"JAN","FEB"))) AS  `POSSESSION`,
        SUM(partner_revenue) AS `REVENUE`,
        SUM(owned_views) as `VIEWS`
        FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
        WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28"
        GROUP BY `DATE`,`VIDEO_ID`
)
GROUP BY `POSSESSION`