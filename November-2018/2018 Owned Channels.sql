--MUSIC
SELECT
        1 as `CMS_ID`,
        channel_id as `CHANNEL_ID`,
        channel_display_name as `CHANNEL_DISPLAY_NAME`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        SUM(partner_revenue) AS `REVENUE`,
        SUM(owned_views) as `VIEWS`
FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UC1bflg6B05dSLKholqGnOMw','UC_ccTaL4kIuAkKOau-hJz6w','UCEd9LOLoVl-QMVMw4aRtTuw','UC2jFHQpJj0j8omCpYYgUcIA','UCj5o5u8CprXYrwTXMJxA8Tw','UCpHLsMt9IkuItftX7nhTBzg','UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCjGZoFtIH9O9Ssvb7FsQOOQ','UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`

UNION ALL

--KIDS
SELECT
        2 as `CMS_ID`,
        channel_id as `CHANNEL_ID`,
        channel_display_name as `CHANNEL_DISPLAY_NAME`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        SUM(partner_revenue) AS `REVENUE`,
        SUM(owned_views) as `VIEWS`
FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UC1bflg6B05dSLKholqGnOMw','UC_ccTaL4kIuAkKOau-hJz6w','UCEd9LOLoVl-QMVMw4aRtTuw','UC2jFHQpJj0j8omCpYYgUcIA','UCj5o5u8CprXYrwTXMJxA8Tw','UCpHLsMt9IkuItftX7nhTBzg','UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCjGZoFtIH9O9Ssvb7FsQOOQ','UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`

UNION ALL

--ENTERTAINMENT
SELECT
        3 as `CMS_ID`,
        channel_id as `CHANNEL_ID`,
        channel_display_name as `CHANNEL_DISPLAY_NAME`,
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        SUM(partner_revenue) AS `REVENUE`,
        SUM(owned_views) as `VIEWS`
FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-09-30" 
AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UC1bflg6B05dSLKholqGnOMw','UC_ccTaL4kIuAkKOau-hJz6w','UCEd9LOLoVl-QMVMw4aRtTuw','UC2jFHQpJj0j8omCpYYgUcIA','UCj5o5u8CprXYrwTXMJxA8Tw','UCpHLsMt9IkuItftX7nhTBzg','UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCjGZoFtIH9O9Ssvb7FsQOOQ','UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
GROUP BY `DATE`,`CHANNEL_ID`,`CHANNEL_DISPLAY_NAME`
