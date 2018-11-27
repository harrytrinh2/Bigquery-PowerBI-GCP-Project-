from google.cloud import bigquery
import os
import pandas as pd
import time
import numpy as np

#TODO: INITIALIZING BIGQUERY CONFIGURATION
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "D:\Codes\pops-dab1c699446f.json"
client = bigquery.Client()
#TODO: PERFORM A QUERY
QUERY = ('''SELECT distinct(video_id),1 as `CMS_ID` FROM `pops-204909.yt_music.p_content_owner_basic_a3_yt_music` WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-19" AND uploader_type = "self"
UNION ALL
SELECT distinct(video_id),2 as `CMS_ID` FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids` WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-19" AND uploader_type = "self"
UNION ALL
SELECT distinct(video_id),3 as `CMS_ID` FROM `pops-204909.yt_entertainment.p_content_owner_basic_a3_yt_entertainment` WHERE DATE(_PARTITIONTIME) between "2018-01-01" and "2018-11-19" AND uploader_type = "self"''')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish
#TODO: READ EXCEL FILE
metadata = pd.read_excel("video_id.xlsx")
data = pd.DataFrame(columns=['video_id','CMS_ID'])
#TODO: Initializers
# n_row = 100000
start_time = time.time()
mynparray = data.values

for i_row,row in enumerate(rows):
    mynparray = np.vstack((mynparray, [row[0],row[1]]))
    if i_row != 0 and i_row % 10000 == 0:
        print round((float(int(i_row) / float(672197)) * 100), 2), "%"
        print("--- %s seconds ---" % (time.time() - start_time))
dataset = pd.DataFrame({'video_id': mynparray[:, 0], 'CMS_ID': mynparray[:, 1]})
dataset.to_excel("video_id.xlsx")
print ("Done!")
