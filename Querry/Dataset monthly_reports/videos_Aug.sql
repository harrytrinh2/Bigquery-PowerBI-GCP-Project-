SELECT distinct video_id, video_title 
FROM (
SELECT TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`, video_id, video_title 
from `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`  
where DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31")