CREATE OR REPLACE TABLE `pops-204909.check_2_projects.delete_duplicates` AS
SELECT * EXCEPT(rn)
FROM (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY CMS_ID, DATE,Total_VIEWS,Total_VIEWS ORDER BY Total_VIEWS ) rn
  FROM `pops-204909.check_2_projects.delete_duplicates`
) 
WHERE rn = 1 

--DELETE WITH FLOAT DATATYPE BY CONVERTING FLOAT TO STRING TEMPORARY
CREATE OR REPLACE TABLE `pops-204909.check_2_projects.top_20_countries_revenue` AS
SELECT * EXCEPT(rn)
FROM 
(
  SELECT *, ROW_NUMBER() OVER(PARTITION BY CMS_ID, DATE,COUNTRY_NAME,CAST(REVENUE as STRING) ORDER BY DATE  ) rn
  FROM `pops-204909.check_2_projects.top_20_countries_revenue`
)
WHERE rn = 1 