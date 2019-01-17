--MUSIC
SELECT 
        1 AS `CMS_ID`,
        DATE_TRUNC(date, MONTH) as `DATE`,
        sum(views) AS `VIEWS`,
        sum(estimated_partner_revenue) AS `REVENUE` 
FROM `pops-204909.2017.YouTube_popsww_M_2017`
WHERE date between "2017-01-01" and "2018-12-31"
GROUP BY `DATE`

UNION ALL
--KIDS
SELECT 
        2 AS `CMS_ID`,
        DATE_TRUNC(date, MONTH) as `DATE`,
        sum(views) AS `VIEWS`,
        sum(estimated_partner_revenue) AS `REVENUE` 
FROM `pops-204909.2017.YouTube_popswwkids_M_2017` 
WHERE date between "2017-01-01" and "2018-12-31"
GROUP BY `DATE`

UNION ALL
--ENTERTAINMENT
SELECT 
        3 AS `CMS_ID`,
        DATE_TRUNC(date, MONTH) as `DATE`,
        sum(views) AS `VIEWS`,
        sum(estimated_partner_revenue) AS `REVENUE` 
FROM `pops-204909.2017.YouTube_popswwchsa_M_2017`  
WHERE date between "2017-01-01" and "2018-12-31"
GROUP BY `DATE`

UNION ALL
--AFFILIATE
SELECT 
        4 AS `CMS_ID`,
        DATE_TRUNC(date, MONTH) as `DATE`,
        sum(views) AS `VIEWS`,
        sum(estimated_partner_revenue) AS `REVENUE` 
FROM `pops-204909.2017.YouTube_popswwchsa_affiliate_M_2017`   
WHERE date between "2017-01-01" and "2018-12-31"
GROUP BY `DATE`