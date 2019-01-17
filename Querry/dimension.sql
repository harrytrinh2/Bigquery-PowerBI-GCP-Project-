SELECT * 
FROM(
        SELECT
            "MUSIC" as `CMS`,
            1 as `CMS_ID`,
            DATE(_PARTITIONTIME) as `DATE`,
            CASE
            WHEN country_code = "VN"
            THEN "LOCAL"
            ELSE "OVERSEAS"
            END AS `LOCATION`,
            SUM(views) AS `EST_VIEWS`,
            SUM(watch_time_minutes) as `WATCH_TIME`,
            SUM(videos_added_to_playlists) as `VIDEOS_ADDED`,
            SUM(subscribers_gained-subscribers_lost) as `SUBSCRIBERS_NET`
    FROM `929791903032.yt_music.p_content_owner_basic_a3_yt_music`
    WHERE DATE(_PARTITIONTIME) >= "2018-01-01"
    GROUP BY `DATE`,`LOCATION`
    ) t1
    RIGHT JOIN `929791903032.dimension.d_date` t2 on t1.DATE = t2.DATEVALUE WHERE t2.YEAR = 2018

SELECT p.Name, p.SS, f.Fear FROM Persons p LEFT JOIN Person_Fear fp ON p.PersonID = fp.PersonID LEFT JOIN Fear f ON f.FearID = fp.FearID