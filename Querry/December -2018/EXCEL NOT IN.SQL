SELECT
      SUM(VIEWS) AS `VIEWS`,
      SUM(REVENUE) AS `REVENUE`
FROM
    (
        SELECT
            1 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            video_id AS `VIDEO_ID`,
            SUM(partner_revenue) AS `REVENUE`,
            SUM(owned_views) as `VIEWS`
    FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
    WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" AND VIDEO_ID NOT IN (SELECT VIDEO_ID FROM `pops-204909.Working.2017_video_id`)
    GROUP BY `DATE`,`VIDEO_ID`

    UNION ALL

    SELECT
            1 as `CMS_ID`,
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            video_id AS `VIDEO_ID`,
            SUM(partner_revenue) AS `REVENUE`,
            SUM(owned_views) as `VIEWS`
    FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
    WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30" AND VIDEO_ID NOT IN (
                                                                                        SELECT
                                                                                            DISTINCT(video_id) AS `VIDEO_ID`
                                                                                        FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
                                                                                        WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-08-31"
                                                                                        )
    GROUP BY `DATE`,`VIDEO_ID`
    )