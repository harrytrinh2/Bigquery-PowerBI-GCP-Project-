  SELECT *
  FROM
  (
    SELECT VIDEO_ID,VIDEO_TITLE,SUM(VIEWS) AS TOTAL_VIEWS
    FROM
    (
        SELECT 
        video_id AS `VIDEO_ID`,
        video_title AS `VIDEO_TITLE`,
        SUM(owned_views) as VIEWS,
        DATE(_PARTITIONTIME) AS `DATE`
        FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
        WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-08-31" 
        GROUP BY `DATE`,`VIDEO_ID`,`VIDEO_TITLE`
    )
    GROUP BY `VIDEO_ID`,`VIDEO_TITLE`
    ORDER BY TOTAL_VIEWS DESC
 )
 WHERE TOTAL_VIEWS>500

 ---chi lay POPS Kids

  SELECT VIDEO_ID,VIDEO_TITLE,SUM(VIEWS) AS TOTAL_VIEWS
FROM
(                   
SELECT 
video_id AS `VIDEO_ID`,
video_title AS `VIDEO_TITLE`,
SUM(owned_views) as VIEWS,
DATE(_PARTITIONTIME) AS `DATE`
FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-08-31" and channel_id ='UC5ezaYrzZpyItPSRG27MLpg'
GROUP BY `DATE`,`VIDEO_ID`,`VIDEO_TITLE`
)
GROUP BY `VIDEO_ID`,`VIDEO_TITLE`
ORDER BY TOTAL_VIEWS DESC