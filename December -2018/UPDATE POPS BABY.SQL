-- DELETE THANG  ESTIMATED
DELETE `pops-204909.CMS_KIDS.POPS_Baby_Basic_2018` WHERE DATE='2018-10-01 00:00:00 UTC'

--INSERT RAW THANG 1->11
INSERT INTO `pops-204909.CMS_KIDS.POPS_Baby_Basic_2018` 
(
  DATE,
  VIDEO_ID,
  VIEWS,
  REVENUE
)

SELECT   
  t0.DATE,
  t0.VIDEO_ID,
  t0.EST_VIEWS as VIEWS,
  IFNULL((t1.REVENUE),0) AS `REVENUE`
FROM
    (
    SELECT
            TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
            video_id as `VIDEO_ID`,
            SUM(views) AS `EST_VIEWS`
    FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`
    WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-30" AND CHANNEL_ID = 'UC7T_bNunNC25G3odVRs_-Eg'
    GROUP BY `DATE`, `VIDEO_ID`
    )t0
LEFT JOIN
    (
    SELECT
        TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) as `DATE`,
        VIDEO_ID,
        IFNULL(SUM(partner_revenue),0) AS `REVENUE`
    FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`
    WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-30" AND CHANNEL_ID = 'UC7T_bNunNC25G3odVRs_-Eg'
    GROUP BY `DATE`,VIDEO_ID
    )t1 on t1.DATE = t0.DATE and t0.VIDEO_ID = t1.VIDEO_ID
WHERE t0.VIDEO_ID IS NOT NULL