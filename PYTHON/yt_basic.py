# -*- coding: utf-8 -*-
import pandas as pd
import os,glob
from google.cloud import bigquery
import time,datetime
import numpy as np

dataset_id = ['yt_music','yt_kids','yt_entertainment','yt_affiliate','yt_th_music','yt_th_entertainment','yt_th_affiliate']
def content_owner_basic_a3(file_name, table_id, dataset_id ):
    data = pd.read_table(file_name,sep=",")
    data = data.replace(np.nan,'',regex=True)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    start_time = time.time()
    _arr = []
    for i_row in range(len(data)):
        _date = data['date'][i_row]
        _channel_id = data['channel_id'][i_row]
        _video_id = data['video_id'][i_row]
        _claimed_status = data['claimed_status'][i_row]
        _uploader_type = data['uploader_type'][i_row]
        _live_or_on_demand = data['live_or_on_demand'][i_row]
        _subscribed_status = data['subscribed_status'][i_row]
        _country_code = data['country_code'][i_row]
        _views = data['views'][i_row]
        _comments = data['comments'][i_row]
        _shares = data['shares'][i_row]
        _watch_time_minutes = data['watch_time_minutes'][i_row]
        _average_view_duration_seconds = data['average_view_duration_seconds'][i_row]
        _average_view_duration_percentage = data['average_view_duration_percentage'][i_row]
        _annotation_impressions = data['annotation_impressions'][i_row]
        _annotation_clickable_impressions = data['annotation_clickable_impressions'][i_row]
        _annotation_clicks = data['annotation_clicks'][i_row]
        _annotation_click_through_rate = data['annotation_click_through_rate'][i_row]
        _annotation_closable_impressions = data['annotation_closable_impressions'][i_row]
        _annotation_closes = data['annotation_closes'][i_row]
        _annotation_close_rate = data['annotation_close_rate'][i_row]
        _card_teaser_impressions = data['card_teaser_impressions'][i_row]
        _card_teaser_clicks = data['card_teaser_clicks'][i_row]
        _card_teaser_click_rate = data['card_teaser_click_rate'][i_row]
        _card_impressions = data['card_impressions'][i_row]
        _card_clicks = data['card_clicks'][i_row]
        _card_click_rate = data['card_click_rate'][i_row]
        _subscribers_gained = data['subscribers_gained'][i_row]
        _subscribers_lost = data['subscribers_lost'][i_row]
        _videos_added_to_playlists = data['videos_added_to_playlists'][i_row]
        _videos_removed_from_playlists = data['videos_removed_from_playlists'][i_row]
        _likes = data['likes'][i_row]
        _dislikes = data['dislikes'][i_row]
        _red_views = data['red_views'][i_row]
        _red_watch_time_minutes = data['red_watch_time_minutes'][i_row]
        _arr.append((_date,_channel_id,_video_id,_claimed_status,_uploader_type,
                    _live_or_on_demand,_subscribed_status,_country_code,_views,_comments,_shares,
                    _watch_time_minutes,_average_view_duration_seconds,_average_view_duration_percentage,
                    _annotation_impressions,_annotation_clickable_impressions,_annotation_clicks,_annotation_click_through_rate,
                    _annotation_closable_impressions,_annotation_closes,_annotation_close_rate,_card_teaser_impressions,
                    _card_teaser_clicks,_card_teaser_click_rate,_card_impressions,_card_clicks,_card_click_rate,
                    _subscribers_gained,_subscribers_lost,_videos_added_to_playlists,_videos_removed_from_playlists,
                    _likes,_dislikes,_red_views,_red_watch_time_minutes))
        if i_row != 0 and i_row % 1500 == 0:
            rows_to_insert = _arr
            errors = client.insert_rows(table, rows_to_insert)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%")
            print("--- %s seconds ---" % (time.time() - start_time))
            time.sleep(5)
            _arr = []
if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\Codes\pops-dab1c699446f.json"
    client = bigquery.Client()
    # print("Importing into MUSIC table...")
    # for fol in glob.glob("L:\\Storage117\\BUSINESS_ANALYSIS\\YouTube reports repository\\2017\\*"):
    #     for _file in glob.glob(os.path.join(fol+"\\", "*.xlsx")):
    #         if "MUSIC_Owner_basic" in _file:
    #             print (_file.split("\\")[-1])
    #             content_owner_basic_a3(file_name=_file,table_id="p_content_owner_basic_a3_yt_kids",dataset_id="yt_kids")
    # print("Imported successfully into KIDS table!")
    print(datetime.datetime.today().strftime('%Y-%m-%d'))