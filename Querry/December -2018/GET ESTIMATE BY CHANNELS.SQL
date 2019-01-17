SELECT  
        t1.DATE, 
        t1.CHANNEL_ID,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
        
FROM   
            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30"  AND uploader_type="self"
            GROUP BY `DATE`, `CHANNEL_ID`
            ) t0             

LEFT JOIN

            (
            SELECT
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  channel_id as `CHANNEL_ID`,
                  IFNULL(SUM(estimated_partner_revenue),0) AS `EST_REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`  
            WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30"  AND uploader_type="self"
            GROUP BY `DATE`, `CHANNEL_ID`
            ) t1 on t1.DATE = t0.DATE and t1.CHANNEL_ID = t0.CHANNEL_ID
            