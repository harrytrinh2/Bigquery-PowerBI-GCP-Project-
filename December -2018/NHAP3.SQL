---Boomerang
SELECT
    DATE_TRUNC(date, MONTH) as `DATE`,
    VIDEO_ID,
    SUM(estimated_partner_revenue) AS `REVENUE`,
    SUM(views) AS `VIEWS`
FROM `pops-204909.2017.YouTube_popswwkids_M_2017`
WHERE date between "2017-01-01" and "2017-12-31" AND channel_id="1psboQLDVCEOD46RNPZdgg"
GROUP BY `DATE`,VIDEO_ID

---kids basic
SELECT
    DATE_TRUNC(date, MONTH) as `DATE`,
    VIDEO_ID,
    SUM(estimated_partner_revenue) AS `REVENUE`,
    SUM(views) AS `VIEWS`
FROM `pops-204909.2017.YouTube_popswwkids_M_2017`
WHERE date between "2017-01-01" and "2017-12-31" AND channel_id="5ezaYrzZpyItPSRG27MLpg"
GROUP BY `DATE`,VIDEO_ID

---POPS Anime Basic
SELECT
    DATE_TRUNC(date, MONTH) as `DATE`,
    VIDEO_ID,
    SUM(estimated_partner_revenue) AS `REVENUE`,
    SUM(views) AS `VIEWS`
FROM `pops-204909.2017.YouTube_popswwkids_M_2017`
WHERE date between "2017-01-01" and "2017-12-31" AND channel_id="kgdDBHO7zl3tWIjldQeK7g"
GROUP BY `DATE`,VIDEO_ID 