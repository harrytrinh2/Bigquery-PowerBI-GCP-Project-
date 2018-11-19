--MUSIC
SELECT  t1.CMS, 
        t0.DATE,
        t1.EST_VIEWS,
        t0.EST_REVENUE
        
FROM   
            (
            SELECT
            'MUSIC' as `CMS`,
            DATE(_PARTITIONTIME) as `DATE`,
            SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) = "2018-10-30"
            GROUP BY `DATE`
            ) t1             

LEFT JOIN

            (
            SELECT
           'MUSIC' as `CMS`,
            DATE(_PARTITIONTIME) as `DATE`,
            SUM(estimated_partner_revenue) AS `EST_REVENUE`                 
            FROM `pops-204909.yt_music.p_content_owner_estimated_revenue_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) = "2018-10-30"
            GROUP BY `DATE`
            ) t0 on t1.CMS = t0.CMS and t1.DATE = t0.DATE    
UNION ALL
--AFF
SELECT  t1.CMS, 
        t0.DATE, 
        t1.EST_VIEWS,
        t0.EST_REVENUE
FROM   
            (
            SELECT
          'AFFILIATE' as `CMS`,
            DATE(_PARTITIONTIME) as `DATE`,
            SUM(views) AS `EST_VIEWS`
            FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) = "2018-10-30" 
            GROUP BY `DATE`
            ) t1             

LEFT JOIN

            (
            SELECT
            'AFFILIATE' as `CMS`,
            DATE(_PARTITIONTIME) as `DATE`,
            SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_affiliate.p_content_owner_estimated_revenue_a1_yt_affiliate`  
            WHERE DATE(_PARTITIONTIME) = "2018-10-30" 
            GROUP BY `DATE`
            ) t0 on t1.CMS = t0.CMS and t1.DATE = t0.DATE 
