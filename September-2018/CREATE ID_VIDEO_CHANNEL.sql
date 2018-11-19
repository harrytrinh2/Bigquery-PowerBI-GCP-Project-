--MUSIC
SELECT 1 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_music.p_content_owner_basic_a3_yt_music`  t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'

UNION ALL

--KIDS
SELECT 2 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_music.p_content_owner_basic_a3_yt_music` t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'

UNION ALL

--ENTERTAINMENT
SELECT 3 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment` t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'

UNION ALL

--AFFILIATE
SELECT 4 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_affiliate.p_content_owner_ad_revenue_raw_a1_yt_affiliate`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_affiliate.p_content_owner_basic_a3_yt_affiliate` t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'

UNION ALL

--MUSIC -TH
SELECT 5 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_th_music.p_content_owner_ad_revenue_raw_a1_yt_th_music`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_th_music.p_content_owner_basic_a3_yt_th_music` t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'

UNION ALL

--ENTERTAINMENT -TH 
SELECT 6 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_th_entertainment.p_content_owner_ad_revenue_raw_a1_yt_th_entertainment`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_th_entertainment.p_content_owner_basic_a3_yt_th_entertainment` t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'

UNION ALL

--AFFILIATE -TH
SELECT 7 as `CMS_ID`,*
FROM
(
SELECT 
        DISTINCT(t0.VIDEO_ID),
        t0.CHANNEL_ID,
        t1.uploader_type
FROM
(
SELECT 
    DISTINCT(VIDEO_ID),
    CHANNEL_ID
FROM `pops-204909.yt_th_affiliate.p_content_owner_ad_revenue_raw_a1_yt_th_affiliate`
WHERE DATE(_PARTITIONTIME) <= "2018-08-31" 
) t0
LEFT JOIN 
`pops-204909.yt_th_affiliate.p_content_owner_basic_a3_yt_th_affiliate` t1
ON t0.VIDEO_ID = t1.VIDEO_ID and t0.CHANNEL_ID=t1.CHANNEL_ID 
)
WHERE uploader_type <> 'thirdParty'