SELECT (EST_REVENUE/EST_MONETIZED_PLAYBACKS*1000/0.55) AS `PLAYBACK_BASED_CPM`,EST_MONETIZED_PLAYBACKS/VIEWS as `PERCENTAGE_OF_PLAYBACK`
FROM
(
SELECT 
SUM(t1.EST_MONETIZED_PLAYBACKS) AS `EST_MONETIZED_PLAYBACKS`,
SUM(t1.EST_REVENUE) AS `EST_REVENUE`, 
SUM(t1.ESTIMATED_PLAYBACK_BASED_CPM) AS `ESTIMATED_PLAYBACK_BASED_CPM`,
SUM(t2.VIEWS) AS `VIEWS`
FROM
    (
    SELECT
        DATE(_PARTITIONTIME) as `DATE`,
        CHANNEL_ID,
        uploader_type,
        SUM(estimated_partner_revenue) AS `EST_REVENUE`,
        SUM(estimated_playback_based_cpm) AS `ESTIMATED_PLAYBACK_BASED_CPM`,
        SUM(estimated_monetized_playbacks) AS `EST_MONETIZED_PLAYBACKS`
    FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
    WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31" and uploader_type<>'thirdParty'
    GROUP BY `DATE`,CHANNEL_ID, uploader_type
    )t1
LEFT JOIN
    (
    SELECT
        DATE(_PARTITIONTIME) as `DATE`,
        CHANNEL_ID,
        uploader_type,
        SUM(views) as `VIEWS`
    FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`   
    WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31" and uploader_type<>'thirdParty'
    GROUP BY `DATE`,CHANNEL_ID,uploader_type
    )t2 
    ON t1.DATE=t2.DATE and t1.CHANNEL_ID=t2.CHANNEL_ID 
)