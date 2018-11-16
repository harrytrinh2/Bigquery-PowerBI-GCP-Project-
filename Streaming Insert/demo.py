# -*- coding: utf-8 -*-
import pandas as pd
import os,glob
import time
from google.cloud import bigquery
import os
import numpy as np

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\Codes\pops-dab1c699446f.json"
# TODO(developer): Uncomment the lines below and replace with your values.
from google.cloud import bigquery
client = bigquery.Client()
print ("Reading file....")
data = pd.read_excel("D:\\YouTube_popswwchsa_affiliate_M_20170101_20170131_videoclaim_rawdata_v1-0.xlsx")
data = data.replace(np.nan, '', regex=True)
data = data[['Date','Video ID', 'Content Type', 'Video Duration (sec)', 'Channel ID','Asset ID', 'Asset Labels', 'Owned Views', 'Partner Revenue']]
data = data.rename(index=str, columns={"Date": "date","Video ID": "video_id", "Content Type": "content_type", "Video Duration (sec)": "average_view_duration_seconds", "Channel ID": "channel_id", "Asset ID": "asset_id", "Asset Labels": "asset_labels", "Owned Views": "views", "Partner Revenue": "estimated_partner_revenue" })
data['date'] = data['date'].dt.strftime('%Y.%m.%d')
data['date'] = data['date'].str.replace(".","-")
print ("Reading file: Done!")
start_time = time.time()
dataset_id = '2017'  # replace with your dataset ID
#For this sample, the table must already exist and have a defined schema
table_id = 'Demo_YouTube'  # replace with your table ID
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)  # API request
_arr = []
for i_row in range(len(data)):
    _date = data['date'][i_row]
    _video_id = data['video_id'][i_row]
    _content_type = (data['content_type'][i_row])
    _average_view_duration_seconds = data['average_view_duration_seconds'][i_row]
    _channel_id = data['channel_id'][i_row]
    _asset_id = data['asset_id'][i_row]
    _asset_labels = data['asset_labels'][i_row].encode('utf8')
    _views = data['views'][i_row]
    _estimated_partner_revenue = data['estimated_partner_revenue'][i_row]
    _arr.append((_date,_video_id,_content_type,str(_average_view_duration_seconds),_channel_id,_asset_id,_asset_labels,str(_views),_estimated_partner_revenue))
    if i_row == 5000:
        break
rows_to_insert = _arr
print (rows_to_insert)
client.insert_rows(table, _arr)  # API request
print ("Imported Successfully")
print("--- %s seconds ---" % (time.time() - start_time))
