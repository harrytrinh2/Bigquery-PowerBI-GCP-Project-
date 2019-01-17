SELECT  t1.CHANNEL_ID,
        t1.EST_VIEWS,
        IFNULL(t0.EST_REVENUE,0) AS REVENUE        
FROM   
            (
            SELECT
                  channel_id AS `CHANNEL_ID`,
                  SUM(views) AS `EST_VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" and uploader_type ="thirdParty"
            GROUP BY `CHANNEL_ID`
            ) t1             

LEFT JOIN

            (
            SELECT
                  channel_id AS `CHANNEL_ID`,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
            FROM `pops-204909.yt_entertainment.p_content_owner_estimated_revenue_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-11-01" and "2018-11-30" and uploader_type ="thirdParty"
            GROUP BY `CHANNEL_ID`
            ) t0 on t1.CHANNEL_ID = t0.CHANNEL_ID  and t1.CHANNEL_ID = t0.CHANNEL_ID