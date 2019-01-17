SELECT channel_id,channel_display_name 
FROM `pops-204909.yt_music.p_content_owner_video_metadata_a1_yt_music` 
WHERE time_uploaded BETWEEN "2018/02/09" AND "2018/11/15" 
GROUP BY channel_id,channel_display_name