SELECT  t0.CMS_ID, 
        t0.DATE,
        t0.LAST_DATE,
        t0.LOCATION, 
        t1.EST_VIEWS, t2.OWNED_VIEWS as RAW_VIEWS, 
        t0.EST_REVENUE, t2.REVENUE as RAW_REVENUE,
        case when t2.OWNED_VIEWS is not null then t2.OWNED_VIEWS else t1.EST_VIEWS end as VIEWS,
        case when t2.REVENUE is not null then t2.REVENUE else t0.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                  --"KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  MAX(DATE(_PARTITIONTIME)) as `LAST_DATE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t0
LEFT JOIN
            (
            SELECT
                  --"KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t1 on t0.CMS_ID = t1.CMS_ID and t0.DATE = t1.DATE and t0.LOCATION = t1.LOCATION
LEFT JOIN 
            (
            SELECT
                  --"KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CASE
                    WHEN country_code = "VN"
                    THEN "LOCAL"
                    ELSE "OVERSEAS"
                  END AS `LOCATION`,
                  SUM(owned_views) AS `OWNED_VIEWS`,
                  SUM(partner_revenue) as `REVENUE`
            FROM `929791903032.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
            GROUP BY `LOCATION`,`DATE`
            ) t2 on t0.CMS_ID = t2.CMS_ID and t0.DATE = t2.DATE and t0.LOCATION = t2.LOCATION