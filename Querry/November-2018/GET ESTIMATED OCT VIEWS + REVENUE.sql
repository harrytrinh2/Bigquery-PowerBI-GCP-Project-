--MUSIC
SELECT  t1.CMS_ID, 
        t0.DATE,
        t0.LOCATION, 
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
            CASE
            WHEN country_code = "VN"
            THEN "LOCAL"
            ELSE "OVERSEAS"
            END AS `LOCATION`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  1 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION
            
UNION ALL


--kids
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t0.LOCATION,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
                  2 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`                  
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  2 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION
                        
UNION ALL


--ent
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t0.LOCATION,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
                  3 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`                  
            FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  3 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION 
                        
UNION ALL


--AFF
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t0.LOCATION,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
                  4 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  4 as `CMS_ID`,
                  CASE
                  WHEN country_code = "VN"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`  
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION
            
UNION ALL
--MUSIC TH
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t0.LOCATION,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
                  5 as `CMS_ID`,
                  CASE
                  WHEN country_code = "TH"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_th_music.p_content_owner_basic_a3_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  5 as `CMS_ID`,
                  CASE
                  WHEN country_code = "TH"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_music.p_content_owner_estimated_revenue_a1_yt_th_music`  
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION
UNION ALL
--ENT TH
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t0.LOCATION,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
                  6 as `CMS_ID`,
                  CASE
                  WHEN country_code = "TH"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  6 as `CMS_ID`,
                  CASE
                  WHEN country_code = "TH"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_entertainment.p_content_owner_estimated_revenue_a1_yt_th_entertainment`  
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION

UNION ALL
--AFF TH
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t0.LOCATION,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
                  7 as `CMS_ID`,
                  CASE
                  WHEN country_code = "TH"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t1             

LEFT JOIN

            (
            SELECT
                  7 as `CMS_ID`,
                  CASE
                  WHEN country_code = "TH"
                  THEN "LOCAL"
                  ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_th_affiliate.p_content_owner_estimated_revenue_a1_yt_th_affiliate`  
            WHERE DATE(_PARTITIONTIME) between "2018-10-01" and "2018-10-31" 
            GROUP BY `DATE`, `LOCATION`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.LOCATION = t0.LOCATION
