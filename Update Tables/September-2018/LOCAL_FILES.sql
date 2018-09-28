SELECT video_id,title,contract_id,category,category_sub1_name,category_sub2_name,season,phase,series_name,updated_at 
FROM `videos` 
WHERE updated_at >= '2018-09-21'

--count rows
SELECT count(video_id)  AS 'ROW_NUMBER'
FROM `videos` 
WHERE updated_at >= '2018-09-21'