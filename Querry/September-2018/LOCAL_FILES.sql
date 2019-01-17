SELECT updated_at,video_id,title,contract_id,category,category_sub1_name,category_sub2_name,season,phase,series_name 
FROM `videos` WHERE updated_at >=  '2018-10-26'


--count rows
SELECT count(video_id)  AS 'ROW_NUMBER'
FROM `videos` 
WHERE updated_at >= '2018-10-03 05:43:41'


--count rows
SELECT count(video_id)  AS 'ROW_NUMBER'
FROM `videos` 
WHERE updated_at >= '2018-10-09'


SELECT id,contract_number FROM `contract` 