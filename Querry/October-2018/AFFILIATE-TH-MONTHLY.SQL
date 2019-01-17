--Affiliate- VN
SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31" 
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28" 
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31"
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30" 
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31" 
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30" 
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"  
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"  
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`

UNION ALL

SELECT
    TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
    channel_id AS `CHANNEL_ID`,
    channel_display_name AS `CHANNEL_DISPLAY_NAME`,
    content_type as `CONTENT_TYPE`,
    SUM(partner_revenue) AS `REVENUE`,
    SUM(owned_views) AS `OWNED_VIEWS`
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) between "2018-09-01" and "2018-09-30"  
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`,`CONTENT_TYPE`