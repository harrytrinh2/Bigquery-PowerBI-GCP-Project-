--MUSIC
SELECT CMS_ID,TITLE,TIME_UPLOADED,TOTAL_VIEWS,RANK
FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,
    time_uploaded AS TIME_UPLOADED,
    TOTAL_VIEWS,
    ROW_NUMBER() OVER (PARTITION BY CMS_ID ORDER BY TOTAL_VIEWS DESC) as `RANK`

FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,SUM(TOTAL_VIEWS) as TOTAL_VIEWS,time_uploaded
FROM(

SELECT     
    CMS_ID,
    VIDEO_ID,
    TITLE,
    DATE,
    TOTAL_VIEWS,time_uploaded
FROM(
SELECT 
    CMS_ID,
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,
    DATE,time_uploaded,
    SUM(EST_VIEWS) AS TOTAL_VIEWS
FROM
(
SELECT CMS_ID,VIDEO_ID,CHANNEL_ID,TITLE,EST_VIEWS,DATE,time_uploaded
FROM
(
SELECT  
        distinct(t0.VIDEO_ID),
        t0.CMS_ID,
        CASE WHEN t0.time_published >='2018-11-23' THEN t0.time_published ELSE t0.time_uploaded END AS `time_uploaded`,
        t0.DATE, 
        t0.CHANNEL_ID,
        t1.EST_VIEWS,
        t0.TITLE
FROM
            ( 
            SELECT
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID, TITLE,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                  DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
            FROM `pops-204909.yt_music.p_content_owner_video_metadata_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,title,time_uploaded,time_published
            ) t0
INNER JOIN
            (
            SELECT
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID,
                  (views) AS `EST_VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,EST_VIEWS
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
)
)
WHERE
    DATE between "2018-11-23" and "2018-11-29"  and time_uploaded between "2018-11-23" and "2018-11-29" 
GROUP BY
    DATE,
    CMS_ID,    
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,time_uploaded
))
GROUP BY VIDEO_ID ,CMS_ID,
    TITLE,time_uploaded
)
LIMIT 50
)

UNION ALL

--KIDS
SELECT CMS_ID,TITLE,TIME_UPLOADED,TOTAL_VIEWS,RANK
FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,
    time_uploaded AS TIME_UPLOADED,
    TOTAL_VIEWS,
    ROW_NUMBER() OVER (PARTITION BY CMS_ID ORDER BY TOTAL_VIEWS DESC) as `RANK`

FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,SUM(TOTAL_VIEWS) as TOTAL_VIEWS,time_uploaded
FROM(

SELECT     
    CMS_ID,
    VIDEO_ID,
    TITLE,
    DATE,
    TOTAL_VIEWS,time_uploaded
FROM(
SELECT 
    CMS_ID,
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,
    DATE,time_uploaded,
    SUM(EST_VIEWS) AS TOTAL_VIEWS
FROM
(
SELECT CMS_ID,VIDEO_ID,CHANNEL_ID,TITLE,EST_VIEWS,DATE,time_uploaded
FROM
(
SELECT  
        distinct(t0.VIDEO_ID),
        t0.CMS_ID,
        CASE WHEN t0.time_published >='2018-11-23' THEN t0.time_published ELSE t0.time_uploaded END AS `time_uploaded`,
        t0.DATE, 
        t0.CHANNEL_ID,
        t1.EST_VIEWS,
        t0.TITLE
FROM
            ( 
            SELECT
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID, TITLE,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                  DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
            FROM `pops-204909.yt_kids.p_content_owner_video_metadata_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,title,time_uploaded,time_published
            ) t0
INNER JOIN
            (
            SELECT
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID,
                  (views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,EST_VIEWS
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
)
)
WHERE
    DATE between "2018-11-23" and "2018-11-29"  and time_uploaded between "2018-11-23" and "2018-11-29" 
GROUP BY 
    DATE,
    CMS_ID,    
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,time_uploaded
))
GROUP BY VIDEO_ID ,CMS_ID,
    TITLE,time_uploaded
)
LIMIT 50
)

UNION ALL

--ENTERTAINMENT
SELECT CMS_ID,TITLE,TIME_UPLOADED,TOTAL_VIEWS,RANK
FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,
    time_uploaded AS TIME_UPLOADED,
    TOTAL_VIEWS,
    ROW_NUMBER() OVER (PARTITION BY CMS_ID ORDER BY TOTAL_VIEWS DESC) as `RANK`

FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,SUM(TOTAL_VIEWS) as TOTAL_VIEWS,time_uploaded
FROM(

SELECT     
    CMS_ID,
    VIDEO_ID,
    TITLE,
    DATE,
    TOTAL_VIEWS,time_uploaded
FROM(
SELECT 
    CMS_ID,
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,
    DATE,time_uploaded,
    SUM(EST_VIEWS) AS TOTAL_VIEWS
FROM
(
SELECT CMS_ID,VIDEO_ID,CHANNEL_ID,TITLE,EST_VIEWS,DATE,time_uploaded
FROM
(
SELECT  
        distinct(t0.VIDEO_ID),
        t0.CMS_ID,
        CASE WHEN t0.time_published >='2018-11-23' THEN t0.time_published ELSE t0.time_uploaded END AS `time_uploaded`,
        t0.DATE, 
        t0.CHANNEL_ID,
        t1.EST_VIEWS,
        t0.TITLE
FROM
            ( 
            SELECT
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID, TITLE,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                  DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
            FROM `pops-204909.yt_entertainment.p_content_owner_video_metadata_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,title,time_uploaded,time_published
            ) t0
INNER JOIN
            (
            SELECT
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID,
                  (views) AS `EST_VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,EST_VIEWS
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
)
)
WHERE
    DATE between "2018-11-23" and "2018-11-29"  and time_uploaded between "2018-11-23" and "2018-11-29" 
GROUP BY
    DATE,
    CMS_ID,    
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,time_uploaded
))
GROUP BY VIDEO_ID ,CMS_ID,
    TITLE,time_uploaded
)
LIMIT 50
)

UNION ALL

--AFFILIATE
SELECT CMS_ID,TITLE,TIME_UPLOADED,TOTAL_VIEWS,RANK
FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,
    time_uploaded AS TIME_UPLOADED,
    TOTAL_VIEWS,
    ROW_NUMBER() OVER (PARTITION BY CMS_ID ORDER BY TOTAL_VIEWS DESC) as `RANK`

FROM(
SELECT     CMS_ID,
    VIDEO_ID,
    TITLE,SUM(TOTAL_VIEWS) as TOTAL_VIEWS,time_uploaded
FROM(

SELECT     
    CMS_ID,
    VIDEO_ID,
    TITLE,
    DATE,
    TOTAL_VIEWS,time_uploaded
FROM(
SELECT 
    CMS_ID,
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,
    DATE,time_uploaded,
    SUM(EST_VIEWS) AS TOTAL_VIEWS
FROM
(
SELECT CMS_ID,VIDEO_ID,CHANNEL_ID,TITLE,EST_VIEWS,DATE,time_uploaded
FROM
(
SELECT  
        distinct(t0.VIDEO_ID),
        t0.CMS_ID,
        CASE WHEN t0.time_published >='2018-11-23' THEN t0.time_published ELSE t0.time_uploaded END AS `time_uploaded`,
        t0.DATE, 
        t0.CHANNEL_ID,
        t1.EST_VIEWS,
        t0.TITLE
FROM
            ( 
            SELECT
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID, TITLE,DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_uploaded)) as time_uploaded,
                  DATE(PARSE_TIMESTAMP("%Y/%m/%d %H:%M:%S", time_published)) as time_published
            FROM `pops-204909.yt_affiliate.p_content_owner_video_metadata_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,title,time_uploaded,time_published
            ) t0
INNER JOIN
            (
            SELECT
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  VIDEO_ID,
                  CHANNEL_ID,
                  (views) AS `EST_VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-11-23" and "2018-11-29" 
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID,EST_VIEWS
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.CHANNEL_ID = t1.CHANNEL_ID and t0.VIDEO_ID = t1.VIDEO_ID
)
)
WHERE
    DATE between "2018-11-23" and "2018-11-29"  and time_uploaded between "2018-11-23" and "2018-11-29" 
GROUP BY
    DATE,
    CMS_ID,    
    VIDEO_ID,
    CHANNEL_ID,
    TITLE,time_uploaded
))
GROUP BY VIDEO_ID ,CMS_ID,
    TITLE,time_uploaded
)
LIMIT 50
)