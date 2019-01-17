--- MUSIC
SELECT
    COUNTRY,
    COUNTRY_NAME,
    SUM(EST_VIEWS) AS `EST_VIEWS`,
    SUM(EST_REVENUE) AS `EST_REVENUE`
FROM
(
    SELECT
            t1.DATE,
            t1.COUNTRY,
            t1.EST_VIEWS,
            t0.EST_REVENUE,
            t3.COUNTRY_NAME
    FROM
            (
            SELECT
                DATE(_PARTITIONTIME) as `DATE`,
                country_code AS `COUNTRY`,
                SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-30" AND "2018-12-06"
            GROUP BY `DATE`, `COUNTRY`
            ) t1
    LEFT JOIN
            (
            SELECT
                DATE(_PARTITIONTIME) as `DATE`,
                country_code AS `COUNTRY`,
                SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) BETWEEN "2018-11-30" AND "2018-12-06"
            GROUP BY `DATE`, `COUNTRY`
            ) t0 on t1.DATE = t0.DATE  AND t1.COUNTRY = t0.COUNTRY
LEFT JOIN `pops-204909.dimension.d_country` t3 on t1.COUNTRY = t3.ISO_CODE_2
)
GROUP BY COUNTRY,COUNTRY_NAME
ORDER BY EST_VIEWS DESC
LIMIT 10