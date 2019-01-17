--MUSIC
select * from
(
select CMS_ID, DATE,CHANNEL_ID,EST_VIEWS, row_number() over(partition by DATE order by EST_VIEWS desc) as rn
from 
(  SELECT
        1 as `CMS_ID`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        channel_id as `CHANNEL_ID`,
        SUM(views) AS `EST_VIEWS`
  FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
  WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-30" 
  GROUP BY `DATE`,`CHANNEL_ID`
)
)A
where rn<=20

UNION ALL

--KIDS
select * from
(
select CMS_ID, DATE,CHANNEL_ID,EST_VIEWS, row_number() over(partition by DATE order by EST_VIEWS desc) as rn
from 
(  SELECT
        2 as `CMS_ID`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        channel_id as `CHANNEL_ID`,
        SUM(views) AS `EST_VIEWS`
  FROM `929791903032.yt_kids.p_content_owner_basic_a3_yt_kids`
  WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-30" 
  GROUP BY `DATE`,`CHANNEL_ID`
)
)A
where rn<=20

UNION ALL

--ENTERTAINMENT
select * from
(
select CMS_ID, DATE,CHANNEL_ID,EST_VIEWS, row_number() over(partition by DATE order by EST_VIEWS desc) as rn
from 
(  SELECT
        3 as `CMS_ID`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        channel_id as `CHANNEL_ID`,
        SUM(views) AS `EST_VIEWS`
  FROM `929791903032.yt_entertainment.p_content_owner_basic_a3_yt_entertainment`
  WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-30" 
  GROUP BY `DATE`,`CHANNEL_ID`
)
)A
where rn<=20