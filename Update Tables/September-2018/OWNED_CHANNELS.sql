INSERT INTO `pops-204909.monthly_reports.owned_channels` 

(
  MONTH,
  CMS,
  CMS_ID,
  DATE,
  CHANNEL_ID,
  CHANNEL_DISPLAY_NAME,
  REVENUE,
  VIEWS
)

SELECT * 
FROM(

--MUSIC
SELECT * FROM(            SELECT
                  "2018-01-01" as `MONTH`,
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
                  "2018-02-01" as `MONTH`,
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
                  "2018-03-01" as `MONTH`,
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
                  "2018-04-01" as `MONTH`,
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
                  "2018-05-01" as `MONTH`,
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
                  "2018-06-01" as `MONTH`,
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
                  "2018-07-01" as `MONTH`,
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

)

UNION ALL
--KIDS
SELECT * FROM(           SELECT
                  "2018-01-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
            SELECT
                  "2018-02-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-03-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-04-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-05-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-06-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-07-01" as `MONTH`,
                  "KIDS" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`   
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31" 
            AND CHANNEL_ID in ('UC5ezaYrzZpyItPSRG27MLpg','UCkgdDBHO7zl3tWIjldQeK7g','UC7T_bNunNC25G3odVRs_-Eg','UCJSqD3sSdMr0mnSwv-_gD4Q','UCjGZoFtIH9O9Ssvb7FsQOOQ')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            )

UNION ALL
--ENTERTAINMENT
SELECT * FROM(           SELECT
                  "2018-01-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
            SELECT
                  "2018-02-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-02-01" and "2018-02-28" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
SELECT
                  "2018-03-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-03-01" and "2018-03-31" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
 SELECT
                  "2018-04-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-04-01" and "2018-04-30" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-05-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-05-01" and "2018-05-31" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-06-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-06-01" and "2018-06-30" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
UNION ALL
          SELECT
                  "2018-07-01" as `MONTH`,
                  "ENTERTAINMENT" as `CMS`,
                  3 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  CHANNEL_DISPLAY_NAME,
                  SUM(partner_revenue) AS `REVENUE`,
                  SUM(owned_views) as `VIEWS`
            FROM `pops-204909.yt_entertainment.p_content_owner_ad_revenue_raw_a1_yt_entertainment`   
            WHERE DATE(_PARTITIONTIME) between "2018-07-01" and "2018-07-31" 
            AND CHANNEL_ID IN ('UC6K3k5O0Dogk1v00beoGMTw','UCIpCJRbm4uwx-Yx7oerY-hg','UCT6xJrO6UwuSahqk9kLG5pA','UCxUTC85GxHI4ZkPn6YvaC4g','UCHK7X17qxIEnCeOlM1xvrCA','UCWRQxDOjKB8zZH8BhggdOVA')
            GROUP BY `DATE`,CHANNEL_ID,CHANNEL_DISPLAY_NAME--,VIDEO_ID
            
)
)