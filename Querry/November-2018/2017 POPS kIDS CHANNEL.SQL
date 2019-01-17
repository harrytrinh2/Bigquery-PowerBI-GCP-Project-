SELECT 
        DATE_TRUNC(date, MONTH) as `DATE`,
        video_id AS `VIDEO_ID`,
        sum(views) AS `VIEWS`,
        sum(estimated_partner_revenue) AS `REVENUE` 
FROM `pops-204909.2017.YouTube_popswwkids_M_2017`
WHERE date between "2017-01-01" and "2018-12-31"  AND channel_id='5ezaYrzZpyItPSRG27MLpg'
GROUP BY `DATE`,`VIDEO_ID`