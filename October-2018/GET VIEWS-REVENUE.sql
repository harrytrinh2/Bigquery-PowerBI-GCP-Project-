--KIDS
SELECT   
        2 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION  
UNION ALL
--MUSIC
SELECT   
        1 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION  

UNION ALL

--ENTERTAINMENT
SELECT   
        3 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION  

UNION ALL
--Affiliate
SELECT   
        4 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION  
UNION ALL
--music -TH
SELECT   
        5 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_music.p_content_owner_estimated_revenue_a1_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_th_music.p_content_owner_basic_a3_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION  
UNION ALL
--ENTERTAINMENT_TH
SELECT   
        6 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_estimated_revenue_a1_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION  
UNION ALL
--Affiliate-TH
SELECT   
        7 as `CMS_ID`,
        t2.LOCATION,
        t0.DATE,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
            GROUP BY `DATE`,`LOCATION`
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                   SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_estimated_revenue_a1_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"   
            GROUP BY `DATE`,`LOCATION`
            ) t1 ON  t0.DATE = t1.DATE   and t1.LOCATION = t0.LOCATION 
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "TH"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30"
            GROUP BY `DATE`,`LOCATION`
            ) t2 on t1.DATE = t2.DATE   and t1.LOCATION = t2.LOCATION