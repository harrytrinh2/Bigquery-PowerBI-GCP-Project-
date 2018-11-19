INSERT INTO `pops-204909.monthly_reports.ugc` 
    (
        CMS_ID,
        DATE,
        ThirdParty_VIEWS,
        Total_VIEWS
    )

--- AFFILIATE
SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS 
FROM
            (
            SELECT
                  --"AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"AFFILIATE" as `CMS`,
                  4 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_affiliate.p_content_owner_basic_a3_yt_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE
			
--- music

union all

SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS
FROM
            (
            SELECT
                  --"music" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"music" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE
--- kids

union all

SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS
FROM
            (
            SELECT
                  --"kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE	

--- entertainment

union all

SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS
FROM
            (
            SELECT
                  --"entertainment" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"entertainment" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE						

--- music-th

union all

SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS
FROM
            (
            SELECT
                  --"music" as `CMS`,
                  5 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_th_music.p_content_owner_basic_a3_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"music" as `CMS`,
                  5 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_th_music.p_content_owner_basic_a3_yt_th_music`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE		

--- entertainment-th

union all

SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS
FROM
            (
            SELECT
                  --"entertainment" as `CMS`,
                  6 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"entertainment" as `CMS`,
                  6 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE			
			
--- affiliate-th

union all

SELECT  t1.CMS_ID, 
        t1.DATE,
        t1.ThirdParty_VIEWS, 
        t2.Total_VIEWS
FROM
            (
            SELECT
                  --"affiliate" as `CMS`,
                  7 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `ThirdParty_VIEWS`
            FROM `929791903032.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
			and uploader_type = "thirdParty"
            GROUP BY `DATE`
            ) t1
LEFT JOIN
			(
            SELECT
                  --"affiliate" as `CMS`,
                  7 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  SUM(views) AS `Total_VIEWS`
            FROM `929791903032.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate`
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31"
            GROUP BY `DATE`
            ) t2 on t1.DATE = t2.DATE				