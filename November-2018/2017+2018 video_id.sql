SELECT video_id,SUM(VIEWS) AS `VIEWS` ,SUM(REVENUE) AS `REVENUE`
FROM
(
    SELECT video_id,SUM(views) AS `VIEWS` ,SUM(estimated_partner_revenue) AS `REVENUE` 
    FROM `pops-204909.2017.YouTube_popsww_M_2017` 
    WHERE content_type="Partner-provided"
    group by video_id

    UNION ALL

    SELECT video_id, SUM(owned_views) AS VIEWS , SUM(partner_revenue) AS `REVENUE`
    FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
    WHERE content_type="Partner-provided" AND DATE(_PARTITIONTIME) BETWEEN "2018-01-01" AND "2018-06-30"
    group by video_id
)
GROUP BY video_id