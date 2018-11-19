INSERT INTO `pops-204909.KIDS.POPS_Kids_Basic` 

(
  CMS_ID ,
  DATE,
  CHANNEL_ID,
  VIDEO_ID,
  VIEWS,
  REVENUE
)

SELECT   
       
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31"   AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31"  AND  CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID

UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28"   AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28"  AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID
            
UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between  "2018-03-01" and "2018-03-31"   AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID

            
UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30"   AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID
            
UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31"  AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID
                        
UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30"  AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID


                                
UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31"  AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID
                                            
UNION ALL

SELECT   
        2 as `CMS_ID`,
        t0.DATE,
        t0.CHANNEL_ID,
        t0.VIDEO_ID,
        case when t0.OWNED_VIEWS is not null then t0.OWNED_VIEWS else t2.EST_VIEWS end as VIEWS,
        case when t0.REVENUE is not null then t0.REVENUE else t1.EST_REVENUE end as REVENUE
FROM
            (
            SELECT
                TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                CHANNEL_ID,
                VIDEO_ID,
                SUM(partner_revenue) AS `REVENUE`,
                SUM(owned_views) AS `OWNED_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t0

LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"  AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t1 ON t0.CHANNEL_ID= t1.CHANNEL_ID and t0.DATE = t1.DATE and t0.VIDEO_ID = t1.VIDEO_ID
LEFT JOIN
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31" AND CHANNEL_ID = 'UC5ezaYrzZpyItPSRG27MLpg'
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID
            ) t2 on t1.DATE = t2.DATE and t1.CHANNEL_ID = t2.CHANNEL_ID and t1.VIDEO_ID = t2.VIDEO_ID
