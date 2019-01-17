--- MUSIC
SELECT
        1 as `CMS_ID`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        t1.CHANNEL_ID,
        t2.CHANNEL_NAME_REPORT,
        SUM(owned_views) AS `VIEWS`,
        SUM(partner_revenue) as `REVENUE`
FROM `929791903032.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music` t1
        INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "MUSIC"
WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-09-30" 
GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT

--- KIDS

UNION ALL
SELECT
        2 as `CMS_ID`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        t1.CHANNEL_ID,
        t2.CHANNEL_NAME_REPORT,
        SUM(owned_views) AS `VIEWS`,
        SUM(partner_revenue) as `REVENUE`
FROM `929791903032.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids` t1
        INNER JOIN `929791903032.dimension.owned_channels` t2 on t1.CHANNEL_ID = t2.CHANNEL_ID_REPORT and t2.CMS = "KIDS"
WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-09-30" 
GROUP BY `DATE`, t1.CHANNEL_ID, t2.CHANNEL_NAME_REPORT