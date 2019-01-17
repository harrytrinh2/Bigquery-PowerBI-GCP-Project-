            SELECT
                  1 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
            SELECT
                 2 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
            SELECT
                  3 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
            SELECT
                  4 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
            SELECT
                  5 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL 
           SELECT
                  6 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL 
           SELECT
                  7 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL 
           SELECT
                  8 as `MONTH`,
                  "MUSIC" as `CMS`,
                  1 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_music.p_content_owner_ad_revenue_raw_a1_yt_music`   
            WHERE DATE(_PARTITIONTIME) between "2018-08-01" and "2018-08-31" 
            AND CHANNEL_ID IN ('UCUgXK2UjZ8G_EM438aYkGrw','UCNbN0PXZeHD3gQAqV1dE9mg','UCpHLsMt9IkuItftX7nhTBzg','UCf1FcgGbvXd7NMak1mq7QAQ','UCuSQdqgTVsVU85PBlsWmibQ','UCj5o5u8CprXYrwTXMJxA8Tw','UC_ccTaL4kIuAkKOau-hJz6w','UC2jFHQpJj0j8omCpYYgUcIA','UCEd9LOLoVl-QMVMw4aRtTuw','UCYe7X1b4oRCqKnxEUZDm_wA')
            --AND COUNTRY_CODE = "VN"
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
