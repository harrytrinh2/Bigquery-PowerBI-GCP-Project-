SELECT   
    t0.CONTENT_TYPE,
    t1.UPLOADER_TYPE,
    t0.VIDEO_ID,
    t0.REVENUE,
    t0.OWNED_VIEWS
FROM
(
SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    content_type as `CONTENT_TYPE`,
    video_id AS `VIDEO_ID`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between  "2018-09-01" and "2018-09-30"   AND `CHANNEL_ID` = 'UC6K3k5O0Dogk1v00beoGMTw'
GROUP BY `DATE`,`CONTENT_TYPE`,`VIDEO_ID`
)t0

LEFT JOIN

(
SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    uploader_type as `UPLOADER_TYPE`
FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between  "2018-09-01" and "2018-09-30"   AND `CHANNEL_ID` = 'UC6K3k5O0Dogk1v00beoGMTw'
GROUP BY `DATE`,`UPLOADER_TYPE`
)t1 ON  t0.DATE = t1.DATE  