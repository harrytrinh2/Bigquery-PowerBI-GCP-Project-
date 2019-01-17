--- MUSIC
    SELECT 
        CHANNEL_ID,
        CHANNEL_DISPLAY_NAME
    FROM `pops-204909.yt_music.p_content_owner_video_metadata_a1_yt_music`
    GROUP BY `CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`
--- KIDS
UNION ALL
    SELECT 
        CHANNEL_ID,
        CHANNEL_DISPLAY_NAME
    FROM `pops-204909.yt_kids.p_content_owner_video_metadata_a1_yt_kids`
    GROUP BY `CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`

--- ENTERTAINMENT

UNION ALL
    SELECT 
        CHANNEL_ID,
        CHANNEL_DISPLAY_NAME
    FROM `pops-204909.yt_entertainment.p_content_owner_video_metadata_a1_yt_entertainment`
    GROUP BY `CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`
    
--- AFFILIATE

UNION ALL
    SELECT 
        CHANNEL_ID,
        CHANNEL_DISPLAY_NAME
    FROM `pops-204909.yt_affiliate.p_content_owner_video_metadata_a1_yt_affiliate`
    GROUP BY `CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`
    