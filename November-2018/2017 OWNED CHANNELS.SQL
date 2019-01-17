--MUSIC
SELECT
		1 AS `CMS_ID`,
		Date as `DATE`,
    SUM(Partner_Revenue) AS `REVENUE`,
    SUM(Owned_views) as `VIEWS`
    FROM `youtube_popsww_m_2017`   
    WHERE DATE between "2017-01-01" and "2017-12-31" AND CHANNEL_ID IN  ('UgXK2UjZ8G_EM438aYkGrw','NbN0PXZeHD3gQAqV1dE9mg','f1FcgGbvXd7NMak1mq7QAQ','uSQdqgTVsVU85PBlsWmibQ','1bflg6B05dSLKholqGnOMw','_ccTaL4kIuAkKOau-hJz6w','Ed9LOLoVl-QMVMw4aRtTuw','2jFHQpJj0j8omCpYYgUcIA','j5o5u8CprXYrwTXMJxA8Tw','pHLsMt9IkuItftX7nhTBzg','5ezaYrzZpyItPSRG27MLpg','kgdDBHO7zl3tWIjldQeK7g','7T_bNunNC25G3odVRs_-Eg','jGZoFtIH9O9Ssvb7FsQOOQ','6K3k5O0Dogk1v00beoGMTw','IpCJRbm4uwx-Yx7oerY-hg','T6xJrO6UwuSahqk9kLG5pA','xUTC85GxHI4ZkPn6YvaC4g','HK7X17qxIEnCeOlM1xvrCA','WRQxDOjKB8zZH8BhggdOVA')
		GROUP BY `DATE`
UNION ALL

--KIDS
SELECT
		2 AS `CMS_ID`,
		Date as `DATE`,
    SUM(Partner_Revenue) AS `REVENUE`,
    SUM(Owned_views) as `VIEWS`
    FROM `youtube_popswwkids_m_2017`   
    WHERE DATE between "2017-01-01" and "2017-12-31" AND CHANNEL_ID IN  ('UgXK2UjZ8G_EM438aYkGrw','NbN0PXZeHD3gQAqV1dE9mg','f1FcgGbvXd7NMak1mq7QAQ','uSQdqgTVsVU85PBlsWmibQ','1bflg6B05dSLKholqGnOMw','_ccTaL4kIuAkKOau-hJz6w','Ed9LOLoVl-QMVMw4aRtTuw','2jFHQpJj0j8omCpYYgUcIA','j5o5u8CprXYrwTXMJxA8Tw','pHLsMt9IkuItftX7nhTBzg','5ezaYrzZpyItPSRG27MLpg','kgdDBHO7zl3tWIjldQeK7g','7T_bNunNC25G3odVRs_-Eg','jGZoFtIH9O9Ssvb7FsQOOQ','6K3k5O0Dogk1v00beoGMTw','IpCJRbm4uwx-Yx7oerY-hg','T6xJrO6UwuSahqk9kLG5pA','xUTC85GxHI4ZkPn6YvaC4g','HK7X17qxIEnCeOlM1xvrCA','WRQxDOjKB8zZH8BhggdOVA')
		GROUP BY `DATE`
		
UNION ALL

--ENTERTAINMENT
SELECT
		3 AS `CMS_ID`,
		Date as `DATE`,
    SUM(Partner_Revenue) AS `REVENUE`,
    SUM(Owned_views) as `VIEWS`
    FROM `youtube_popswwchsa_m_2017`   
    WHERE DATE between "2017-01-01" and "2017-12-31" AND CHANNEL_ID IN  ('UgXK2UjZ8G_EM438aYkGrw','NbN0PXZeHD3gQAqV1dE9mg','f1FcgGbvXd7NMak1mq7QAQ','uSQdqgTVsVU85PBlsWmibQ','1bflg6B05dSLKholqGnOMw','_ccTaL4kIuAkKOau-hJz6w','Ed9LOLoVl-QMVMw4aRtTuw','2jFHQpJj0j8omCpYYgUcIA','j5o5u8CprXYrwTXMJxA8Tw','pHLsMt9IkuItftX7nhTBzg','5ezaYrzZpyItPSRG27MLpg','kgdDBHO7zl3tWIjldQeK7g','7T_bNunNC25G3odVRs_-Eg','jGZoFtIH9O9Ssvb7FsQOOQ','6K3k5O0Dogk1v00beoGMTw','IpCJRbm4uwx-Yx7oerY-hg','T6xJrO6UwuSahqk9kLG5pA','xUTC85GxHI4ZkPn6YvaC4g','HK7X17qxIEnCeOlM1xvrCA','WRQxDOjKB8zZH8BhggdOVA')
		GROUP BY `DATE`