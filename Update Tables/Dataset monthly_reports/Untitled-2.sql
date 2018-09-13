            SELECT
                  "kids" as `CMS`,
                  2 as `CMS_ID`,
                  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
                  CHANNEL_ID,
                  VIDEO_ID,
                  SUM(estimated_partner_revenue) AS `EST_REVENUE`
            FROM `pops-204909.yt_kids.p_content_owner_estimated_revenue_a1_yt_kids`
            WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-01-31"
            GROUP BY `DATE`,CHANNEL_ID,VIDEO_ID