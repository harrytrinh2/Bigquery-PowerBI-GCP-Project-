
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t1.EST_VIEWS,
        t0.EST_REVENUE,
        t1.CHANNEL_ID
FROM   
            (
            SELECT
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(views) AS `EST_VIEWS`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID`, `UPLOADER_TYPE`
            ) t1             

LEFT JOIN

            (
            SELECT
                  1 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID` , `UPLOADER_TYPE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.CHANNEL_ID=t0.CHANNEL_ID 
            
UNION ALL


--kids
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t1.EST_VIEWS,
        t0.EST_REVENUE,
        t1.CHANNEL_ID
FROM   
            (
            SELECT
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(views) AS `EST_VIEWS`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID`, `UPLOADER_TYPE`
            ) t1             

LEFT JOIN

            (
            SELECT
                  2 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID` , `UPLOADER_TYPE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.CHANNEL_ID=t0.CHANNEL_ID 
                        
UNION ALL


--ent
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t1.EST_VIEWS,
        t0.EST_REVENUE,
        t1.CHANNEL_ID
FROM   
            (
            SELECT
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(views) AS `EST_VIEWS`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID`, `UPLOADER_TYPE`
            ) t1             

LEFT JOIN

            (
            SELECT
                  3 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID` , `UPLOADER_TYPE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.CHANNEL_ID=t0.CHANNEL_ID 
                        
UNION ALL


--AFF
SELECT  t1.CMS_ID, 
        t0.DATE, 
        t1.EST_VIEWS,
        t0.EST_REVENUE,
        t1.CHANNEL_ID
FROM   
            (
            SELECT
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(views) AS `EST_VIEWS`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID`, `UPLOADER_TYPE`
            ) t1             

LEFT JOIN

            (
            SELECT
                  4 as `CMS_ID`,
                  DATE(_PARTITIONTIME) as `DATE`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`,
                  channel_id as `CHANNEL_ID`,
                  uploader_type AS `UPLOADER_TYPE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`  
            WHERE DATE(_PARTITIONTIME) between "2018-10-19" and "2018-10-25" and uploader_type='self'
            GROUP BY `DATE`,`CHANNEL_ID` , `UPLOADER_TYPE`
            ) t0 on t1.CMS_ID = t0.CMS_ID and t1.DATE = t0.DATE and t1.CHANNEL_ID=t0.CHANNEL_ID 
            