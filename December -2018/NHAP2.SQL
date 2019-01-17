SELECT * 
FROM
    (
    --MUSIC
    SELECT CMS_ID, SUM(EST_VIEWS) AS TOTAL_VIEWS
    FROM
    (
    SELECT  
            distinct(t0.VIDEO_ID),
            t0.CMS_ID,
            CASE WHEN t0.time_published >='2018-12-28'  THEN t0.time_published ELSE t0.time_uploaded END AS `TIME_UPLOADED`,
            t1.EST_VIEWS,
            t0.video_title
    FROM
                ( 
                SELECT
                    1 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    video_title,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                    DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
                FROM `pops-204909.yt_music.p_content_owner_video_metadata_a2_yt_music`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,video_title,time_uploaded,time_published
                ) t0
    INNER JOIN
                (
                SELECT
                    1 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    (views) AS `EST_VIEWS`
                FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,EST_VIEWS
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
    )
    WHERE
        time_uploaded between "2018-12-28" and "2019-01-01"
    GROUP BY CMS_ID

    UNION ALL

    --KIDS
    SELECT CMS_ID, SUM(EST_VIEWS) AS TOTAL_VIEWS
    FROM
    (
    SELECT  
            distinct(t0.VIDEO_ID),
            t0.CMS_ID,
            CASE WHEN t0.time_published >='2018-12-28'  THEN t0.time_published ELSE t0.time_uploaded END AS `TIME_UPLOADED`,
            t1.EST_VIEWS,
            t0.video_title
    FROM
                ( 
                SELECT
                    2 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    video_title,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                    DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
                FROM `pops-204909.yt_kids.p_content_owner_video_metadata_a2_yt_kids`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,video_title,time_uploaded,time_published
                ) t0
    INNER JOIN
                (
                SELECT
                    2 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    (views) AS `EST_VIEWS`
                FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,EST_VIEWS
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
    )
    WHERE
        time_uploaded between "2018-12-28" and "2019-01-01"
    GROUP BY CMS_ID

    UNION ALL

    --ENTERTAINMENT
    SELECT CMS_ID, SUM(EST_VIEWS) AS TOTAL_VIEWS
    FROM
    (
    SELECT  
            distinct(t0.VIDEO_ID),
            t0.CMS_ID,
            CASE WHEN t0.time_published >='2018-12-28'  THEN t0.time_published ELSE t0.time_uploaded END AS `TIME_UPLOADED`,
            t1.EST_VIEWS,
            t0.video_title
    FROM
                ( 
                SELECT
                    3 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    video_title,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                    DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
                FROM `pops-204909.yt_entertainment.p_content_owner_video_metadata_a2_yt_entertainment`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,video_title,time_uploaded,time_published
                ) t0
    INNER JOIN
                (
                SELECT
                    3 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    (views) AS `EST_VIEWS`
                FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,EST_VIEWS
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
    )
    WHERE
        time_uploaded between "2018-12-28" and "2019-01-01"
    GROUP BY CMS_ID

    UNION ALL

    --AFFILIATE
    SELECT CMS_ID, SUM(EST_VIEWS) AS TOTAL_VIEWS
    FROM
    (
    SELECT  
            distinct(t0.VIDEO_ID),
            t0.CMS_ID,
            CASE WHEN t0.time_published >='2018-12-28'  THEN t0.time_published ELSE t0.time_uploaded END AS `TIME_UPLOADED`,
            t1.EST_VIEWS,
            t0.video_title
    FROM
                (
                SELECT
                    4 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    video_title,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                    DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
                FROM `pops-204909.yt_affiliate.p_content_owner_video_metadata_a2_yt_affiliate`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,video_title,time_uploaded,time_published
                ) t0
    INNER JOIN
                (
                SELECT
                    4 as `CMS_ID`,
                    DATE(_PARTITIONTIME) as `DATE`,
                    VIDEO_ID,
                    (views) AS `EST_VIEWS`
                FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
                WHERE DATE(_PARTITIONTIME) between "2018-12-28" and "2019-01-01" 
                GROUP BY `DATE`,VIDEO_ID,EST_VIEWS
                ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
    )
    WHERE
        time_uploaded between "2018-12-28" and "2019-01-01"
    GROUP BY CMS_ID
)
ORDER BY CMS_ID ASC