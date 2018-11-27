# -*- coding: utf-8 -*-
import pandas as pd
import os,glob
from google.cloud import bigquery
import time
import numpy as np

def insert_into_Bigquery(file_name, table_id, dataset_id = '2017'):
    # TODO: CLEANING DATA
    print ("Reading file....")
    data = pd.read_excel(file_name)
    data = data.replace(np.nan, '', regex=True)
    data = data[['Date', 'Video ID', 'Content Type', 'Video Duration (sec)', 'Channel ID', 'Asset ID', 'Asset Labels',
                 'Owned Views', 'Partner Revenue']]
    data = data.rename(index=str, columns={"Date": "date", "Video ID": "video_id", "Content Type": "content_type",
                                           "Video Duration (sec)": "average_view_duration_seconds",
                                           "Channel ID": "channel_id", "Asset ID": "asset_id",
                                           "Asset Labels": "asset_labels", "Owned Views": "views",
                                           "Partner Revenue": "estimated_partner_revenue"})
    data['date'] = data['date'].dt.strftime('%Y.%m.%d')
    data['date'] = data['date'].str.replace(".", "-")
    print ("Reading file: Done!")
    start_time = time.time()
    # TODO: PROVIDE CONFIGURATION
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    # TODO: Get data and import into Bigquery
    _arr = []
    for i_row in range(len(data)):
        _date = data['date'][i_row]
        _video_id = data['video_id'][i_row]
        _content_type = (data['content_type'][i_row])
        _average_view_duration_seconds = data['average_view_duration_seconds'][i_row]
        _channel_id = data['channel_id'][i_row]
        _asset_id = data['asset_id'][i_row]
        try:
            _asset_labels = data['asset_labels'][i_row].encode('utf8')
        except:
            _asset_labels = str(data['asset_labels'][i_row])
        _views = data['views'][i_row]
        _estimated_partner_revenue = data['estimated_partner_revenue'][i_row]
        _arr.append((_date, _video_id, _content_type, str(_average_view_duration_seconds), _channel_id, _asset_id, _asset_labels, str(_views), _estimated_partner_revenue))
        if i_row != 0 and i_row % 1500 == 0:
            rows_to_insert = _arr
            errors = client.insert_rows(table, rows_to_insert)
            assert errors == []
            print round((float(int(i_row) / float(len(data))) * 100), 2), "%"
            print("--- %s seconds ---" % (time.time() - start_time))
            time.sleep(5)
            _arr = []
if __name__ == '__main__':
    # TODO: INITIALIZE BIGQUERY
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\Codes\pops-dab1c699446f.json"
    client = bigquery.Client()
    # TODO: READING FILES
    #insert_into_Bigquery(file_name="L:\\Storage117\\BUSINESS_ANALYSIS\\YouTube reports repository\\2017\\02-Feb\\YouTube_popswwkids_M_20170201_20170228_videoclaim_rawdata_v1-0.xlsx", table_id="YouTube_popswwkids_M_2017")
    #insert_into_Bigquery(file_name="D:\\YouTube_popswwchsa_affiliate_M_20170101_20170131_videoclaim_rawdata_v1-0.xlsx", table_id="YouTube_popswwkids_M_2017")

    # print("Importing into KIDS table...")
    # for fol in glob.glob("L:\\Storage117\\BUSINESS_ANALYSIS\\YouTube reports repository\\2017\\*"):
    #     for _file in glob.glob(os.path.join(fol+"\\", "*.xlsx")):
    #         if "YouTube_popswwkids_M_2017" in _file:
    #             print (_file.split("\\")[-1])
    #             insert_into_Bigquery(file_name=_file,table_id="YouTube_popswwkids_M_2017")
    # print("Imported successfully into KIDS table!")

    print("Importing into ENTERTAINMENT table...")
    for fol in glob.glob("L:\\Storage117\\BUSINESS_ANALYSIS\\YouTube reports repository\\2017\\*"):
        for _file in glob.glob(os.path.join(fol+"\\", "*.xlsx")):
            if "YouTube_popswwchsa_M_2017" in _file:
                print (_file.split("\\")[-1])
                insert_into_Bigquery(file_name=_file,table_id="YouTube_popswwchsa_M_2017")
    print("Imported successfully into ENTERTAINMENT table!")

    # print("Importing into AFFILIATE table...")
    # for fol in glob.glob("L:\\Storage117\\BUSINESS_ANALYSIS\\YouTube reports repository\\2017\\*"):
    #     for _file in glob.glob(os.path.join(fol+"\\", "*.xlsx")):
    #         if "YouTube_popswwchsa_affiliate_M_2017" in _file:
    #             print (_file.split("\\")[-1])
    #             insert_into_Bigquery(file_name=_file,table_id="YouTube_popswwchsa_affiliate_M_2017")
    # print("Imported successfully into AFFILIATE table!")

    # print("Importing into MUSIC table...")
    # for fol in glob.glob("L:\\Storage117\\BUSINESS_ANALYSIS\\YouTube reports repository\\2017\\*"):
    #     for _file in glob.glob(os.path.join(fol+"\\", "*.xlsx")):
    #         if "YouTube_popsww_M_2017" in _file:
    #             print (_file.split("\\")[-1])
    #             insert_into_Bigquery(file_name=_file,table_id="YouTube_popsww_M_2017")
    # print("Imported successfully into MUSIC table!")