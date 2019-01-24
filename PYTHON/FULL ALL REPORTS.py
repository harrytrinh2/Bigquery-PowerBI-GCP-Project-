#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from io import FileIO
import socket
from google.cloud import bigquery
from datetime import timedelta
from datetime import datetime
import time
import pandas as pd
import numpy as np
import httplib2
from oauth2client import client, GOOGLE_TOKEN_URI

CLIENT_SECRETS_FILE = "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']
API_SERVICE_NAME = 'youtubereporting'
API_VERSION = 'v1'
CLIENT_ID = "929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com"
CLIENT_SECRET = "YHDd4FrEFtqjhIkZhprwUMuy"
REFRESH_TOKEN = "1/RinJvsjGrAUvBj3QoHsHMvopmsf-7U0x1KCvhpo0cq0"
ACCESS_TOKEN = "ya29.GlubBs2CfFIMOsQRkqSxgAyff5rQ8aiu1IWI6j2Ery5MsuL4VOnr9s7owicF0C_vgM8USc1IDY03jXxWlQn7dCjn2MMa5Gzh6LWZlxqLdLnU2ib8YXPR8nialM1F"
credentials = client.OAuth2Credentials(
    access_token = ACCESS_TOKEN,
    client_id = CLIENT_ID,
    client_secret = CLIENT_SECRET,
    refresh_token = REFRESH_TOKEN,
    token_expiry = 3600,
    token_uri = "https://oauth2.googleapis.com/token",
    scopes= "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    user_agent="Bearer",
    revoke_uri= None)
# Authorize the request and store authorization credentials.
def get_authenticated_service():
    # flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    # credentials = flow.run_local_server()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

# Remove keyword arguments that are not set.
def remove_empty_kwargs(**kwargs):
    good_kwargs = {}
    if kwargs is not None:
        for key, value in kwargs.items():
            if value:
                good_kwargs[key] = value
    return good_kwargs

# Call the YouTube Reporting API's reports.list method to retrieve reports created by a job.
def retrieve_reports(youtube_reporting,lastest_date_BQ, **kwargs):
    # Only include the onBehalfOfContentOwner keyword argument if the user
    # set a value for the --content_owner argument.
    kwargs = remove_empty_kwargs(**kwargs)
    # Retrieve available reports for the selected job.
    results = youtube_reporting.jobs().reports().list(**kwargs).execute()
    downloadUrl = ''
    if 'reports' in results and results['reports']:
        reports = results['reports']
        _startTime = []
        _createTime = []
        _downloadUrl = []
        _index = []
        for index, i in enumerate(reports):
            _startTime.append(datetime.strptime(i['startTime'].split("T")[0], "%Y-%m-%d"))
            _createTime.append(datetime.strptime(i['createTime'].split("T")[0], "%Y-%m-%d"))
            _downloadUrl.append(i['downloadUrl'])
            if lastest_date_BQ < datetime.strptime(i['startTime'].split("T")[0], "%Y-%m-%d"):
                _index.append(index)
        _index = sorted(_index, reverse=True)
        _startTime_final = []
        _createTime_final = []
        _downloadUrl_final = []
        if len(_index) == 0:
            print()
        elif len(_index) > 0:
            for i in _index:
                _startTime_final.append(_startTime[i])
                _createTime_final.append(_createTime[i])
                _downloadUrl_final.append(_downloadUrl[i])
        _dict_retrieve = {}
        for index in range(len(_startTime_final)):
            _dict_retrieve[_startTime_final[index]] = [_createTime_final[index], _downloadUrl_final[index]]
        return _dict_retrieve

# Call the YouTube Reporting API's media.download method to download the report.
def download_report(youtube_reporting, report_url, local_file):
    request = youtube_reporting.media().download(
        resourceName=' ')
    request.uri = report_url
    fh = FileIO(local_file, mode='wb')
    # Stream/download the report in a single request.
    downloader = MediaIoBaseDownload(fh, request, chunksize=-1)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        if status:
            print('Download %d%%.' % int(status.progress() * 100))
    print('Download Complete!')

def get_latest_date_func(content_owner,table):
    query_job = client.query('''SELECT MAX( PARSE_DATE('%Y%m%d',date))  AS `lastest_date_BQ`
                             FROM `pops-204909.''' + content_owner + "." + table + "`")
    rows = query_job.result()
    for i_row in rows:
        _lastest_date_BQ = i_row.get('lastest_date_BQ')
        return datetime(_lastest_date_BQ.year,_lastest_date_BQ.month,_lastest_date_BQ.day)

def check_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

def get_lastest_month_func(content_owner,table):
    query_job = client.query('''SELECT MAX( PARSE_DATE('%Y%m%d',date))  AS `newest_month_BG`
                             FROM `pops-204909.''' + content_owner + "." + table + "`")
    rows = query_job.result()
    for i_row in rows:
        _newest_month_BG = i_row.get('newest_month_BG')
        return datetime(int(str(_newest_month_BG).split("-")[0]),int(str(_newest_month_BG).split("-")[1]),1)

def remove_if_not_complete(content_owner,table,_date):
    try:
        query_job = client.query('''DELETE FROM `pops-204909.''' + content_owner + "." + table + "`" + " WHERE date='"+_date.replace("-","")+"'")
        rows = query_job.result()
    except:
        pass

def check_upload(file_name,content_owner,table,_date):
    data = pd.read_table(file_name, sep=",")
    data = data.replace(np.nan, '', regex=True)
    print(file_name,_date.replace("-",""))
    try:
        a = '''SELECT count(*) as `row_number` FROM `pops-204909.''' + content_owner + "." + table + "`" + " WHERE date='"+_date.replace("-","")+"'"
        print(a)
        query_job = client.query('''SELECT count(*) as `row_number` FROM `pops-204909.''' + content_owner + "." + table + "`" + " WHERE date='"+_date.replace("-","")+"'")
        rows = query_job.result()
        for i_row in rows:
            _newest_month_BG = i_row.get('row_number')
            print("_newest_month_BG = ",int(_newest_month_BG),"     ","len(data)= ",len(data))
            if int(_newest_month_BG) == len(data):
                os.remove(file_name)
            else:
                remove_if_not_complete(content_owner=content_owner,table=table,_date=_date)
    except (ValueError,KeyError) as e:
        print("Could not remove the file!")
def p_content_owner_basic_a3(file_name, dataset_id,_content_owner,_table,_date):
    data = pd.read_table(file_name, sep=",")
    data = data.replace(np.nan, '', regex=True)
    table_ref = client.dataset(dataset_id).table(_table)
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
        _arr.append((str(_date), str(_channel_id), str(_video_id), str(_claimed_status), str(_uploader_type),
                     str(_live_or_on_demand), str(_subscribed_status), str(_country_code), int(_views), int(_comments), int(_shares),
                     float(_watch_time_minutes), float(_average_view_duration_seconds), float(_average_view_duration_percentage),
                     int(_annotation_impressions), int(_annotation_clickable_impressions), int(_annotation_clicks),
                     float(_annotation_click_through_rate),
                     int(_annotation_closable_impressions), int(_annotation_closes), float(_annotation_close_rate),
                     int(_card_teaser_impressions),int(_card_teaser_clicks), float(_card_teaser_click_rate), int(_card_impressions), int(_card_clicks), float(_card_click_rate),
                     int(_subscribers_gained), int(_subscribers_lost), int(_videos_added_to_playlists), int(_videos_removed_from_playlists),
                         int(_likes), int(_dislikes), int(_red_views), float(_red_watch_time_minutes)))
        if i_row != 0 and i_row % 1500 == 0:
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%" ,"------- %s seconds -------" % round((time.time() - start_time),2))
            time.sleep(5)
            _arr = []
    if len(_arr) > 0:
        errors = client.insert_rows(table, _arr)
        assert errors == []
        print("------- Done! 100% Uploaded. -------")
        time.sleep(5)
        _arr = []

def p_content_owner_estimated_revenue_a1(file_name, dataset_id,_content_owner,_table,_date):
    data = pd.read_table(file_name, sep=",")
    data = data.replace(np.nan, '', regex=True)
    table_ref = client.dataset(dataset_id).table(_table)
    table = client.get_table(table_ref)
    start_time = time.time()
    _arr = []
    for i_row in range(len(data)):
        _date = data['date'][i_row]
        _channel_id = data['channel_id'][i_row]
        _video_id = data['video_id'][i_row]
        _claimed_status = data['claimed_status'][i_row]
        _uploader_type = data['uploader_type'][i_row]
        _country_code = data['country_code'][i_row]
        _estimated_partner_revenue = data['estimated_partner_revenue'][i_row]
        _estimated_partner_ad_revenue = data['estimated_partner_ad_revenue'][i_row]
        _estimated_partner_ad_auction_revenue = data['estimated_partner_ad_auction_revenue'][i_row]
        _estimated_partner_ad_reserved_revenue = data['estimated_partner_ad_reserved_revenue'][i_row]
        _estimated_youtube_ad_revenue = data['estimated_youtube_ad_revenue'][i_row]
        _estimated_monetized_playbacks = data['estimated_monetized_playbacks'][i_row]
        _estimated_playback_based_cpm = data['estimated_playback_based_cpm'][i_row]
        _ad_impressions = data['ad_impressions'][i_row]
        _estimated_cpm = data['estimated_cpm'][i_row]
        _estimated_partner_red_revenue = data['estimated_partner_red_revenue'][i_row]
        _estimated_partner_transaction_revenue = data['estimated_partner_transaction_revenue'][i_row]
        _arr.append(
            (str(_date), str(_channel_id), str(_video_id), str(_claimed_status), str(_uploader_type), str(_country_code),
             float(_estimated_partner_revenue),float(_estimated_partner_ad_revenue), float(_estimated_partner_ad_auction_revenue),
             float(_estimated_partner_ad_reserved_revenue),
             float(_estimated_youtube_ad_revenue), int(_estimated_monetized_playbacks), float(_estimated_playback_based_cpm),
             int(_ad_impressions), float(_estimated_cpm), float(_estimated_partner_red_revenue),float(_estimated_partner_transaction_revenue)))
        if i_row != 0 and i_row % 1500 == 0:
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%",
                  "------- %s seconds -------" % round((time.time() - start_time), 2))
            time.sleep(5)
            _arr = []
    if len(_arr) > 0:
        errors = client.insert_rows(table, _arr)
        assert errors == []
        print("------- Done! 100% Uploaded. -------")
        time.sleep(5)
        _arr = []
def p_content_owner_ad_revenue_raw_a1(file_name, dataset_id,_content_owner,_table,_date):
    data = pd.read_table(file_name, sep=",")
    data = data.replace(np.nan, '', regex=True)
    table_ref = client.dataset(dataset_id).table(_table)
    table = client.get_table(table_ref)
    start_time = time.time()
    _arr = []
    for i_row in range(len(data)):
        _adjustment_type = data['adjustment_type'][i_row]
        _date = data['date'][i_row]
        _country_code = data['country_code'][i_row]
        _video_id = data['video_id'][i_row]
        _video_title = data['video_title'][i_row]
        _video_duration_sec = data['video_duration_sec'][i_row]
        _category = data['category'][i_row]
        _channel_id = data['channel_id'][i_row]
        _uploader = data['uploader'][i_row]
        _channel_display_name = data['channel_display_name'][i_row]
        _content_type = data['content_type'][i_row]
        _policy = data['policy'][i_row]
        _owned_views = data['owned_views'][i_row]
        _youtube_revenue_split_auction = data['youtube_revenue_split_auction'][i_row]
        _youtube_revenue_split_reserved = data['youtube_revenue_split_reserved'][i_row]
        _youtube_revenue_split_partner_sold_youtube_served = data['youtube_revenue_split_partner_sold_youtube_served'][
            i_row]
        _youtube_revenue_split_partner_sold_partner_served = data['youtube_revenue_split_partner_sold_partner_served'][
            i_row]
        _youtube_revenue_split = data['youtube_revenue_split'][i_row]
        _partner_revenue_auction = data['partner_revenue_auction'][i_row]
        _partner_revenue_reserved = data['partner_revenue_reserved'][i_row]
        _partner_revenue_partner_sold_youtube_served = data['partner_revenue_partner_sold_youtube_served'][i_row]
        _partner_revenue_partner_sold_partner_served = data['partner_revenue_partner_sold_partner_served'][i_row]
        _partner_revenue = data['partner_revenue'][i_row]

        _arr.append((str(_adjustment_type), str(_date),str(_country_code) ,str(_video_id), str(_video_title), int(_video_duration_sec),str(_category), str(_channel_id),
                     str(_uploader), str(_channel_display_name), str(_content_type), str(_policy),
                     int(_owned_views), float(_youtube_revenue_split_auction), float(_youtube_revenue_split_reserved),
                     float(_youtube_revenue_split_partner_sold_youtube_served),
                     float(_youtube_revenue_split_partner_sold_partner_served),
                     float(_youtube_revenue_split), float(_partner_revenue_auction), float(_partner_revenue_reserved),
                     float(_partner_revenue_partner_sold_youtube_served), float(_partner_revenue_partner_sold_partner_served),
                     float(_partner_revenue)))
        if i_row != 0 and i_row % 1500 == 0:
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%",
                  "------- %s seconds -------" % round((time.time() - start_time), 2))
            time.sleep(5)
            _arr = []
    if len(_arr) > 0:
        errors = client.insert_rows(table, _arr)
        assert errors == []
        print("------- Done! 100% Uploaded. -------")
        time.sleep(5)
        _arr = []

def p_content_owner_video_metadata_a2(file_name, dataset_id,_content_owner,_table,_date):
    data = pd.read_table(file_name, sep=",")
    data = data.replace(np.nan, '', regex=True)
    table_ref = client.dataset(dataset_id).table(_table)
    table = client.get_table(table_ref)
    start_time = time.time()
    _arr = []
    for i_row in range(len(data)):
        _video_id = data['video_id'][i_row]
        _channel_id = data['channel_id'][i_row]
        _channel_display_name = data['channel_display_name'][i_row]
        _time_uploaded = data['time_uploaded'][i_row]
        _time_published = data['time_published'][i_row]
        _video_title = data['video_title'][i_row]
        _video_length = data['video_length'][i_row]
        _views = data['views'][i_row]
        _comments = data['comments'][i_row]
        _video_privacy_status = data['video_privacy_status'][i_row]
        _video_url = data['video_url'][i_row]
        _category = data['category'][i_row]
        _embedding_allowed = data['embedding_allowed'][i_row]
        _ratings_allowed = data['ratings_allowed'][i_row]
        _comments_allowed = data['comments_allowed'][i_row]
        _claim_origin = data['claim_origin'][i_row]
        _content_type = data['content_type'][i_row]
        _upload_source = data['upload_source'][i_row]
        _claimed_by_this_owner = data['claimed_by_this_owner'][i_row]
        _claimed_by_another_owner = data['claimed_by_another_owner'][i_row]
        _other_owners_claiming = data['other_owners_claiming'][i_row]
        _offweb_syndicatable = data['offweb_syndicatable'][i_row]
        _claim_id = data['claim_id'][i_row]
        _asset_id = data['asset_id'][i_row]
        _custom_id = data['custom_id'][i_row]
        _effective_policy = data['effective_policy'][i_row]
        _third_party_video_id = data['third_party_video_id'][i_row]
        _third_party_ads_enabled = data['third_party_ads_enabled'][i_row]
        _display_ads_enabled = data['display_ads_enabled'][i_row]
        _sponsored_cards_enabled = data['sponsored_cards_enabled'][i_row]
        _overlay_ads_enabled = data['overlay_ads_enabled'][i_row]
        _nonskippable_video_ads_enabled = data['nonskippable_video_ads_enabled'][i_row]
        _long_nonskippable_video_ads_enabled = data['long_nonskippable_video_ads_enabled'][i_row]
        _skippable_video_ads_enabled = data['skippable_video_ads_enabled'][i_row]
        _prerolls_enabled = data['prerolls_enabled'][i_row]
        _postrolls_enabled = data['postrolls_enabled'][i_row]
        _isrc = data['isrc'][i_row]
        _eidr = data['eidr'][i_row]
        _date = _date.replace("-","")
        _arr.append((str(_date),str(_video_id), str(_channel_id), str(_channel_display_name), str(_time_uploaded), str(_time_published), str(_video_title),
                     int(_video_length),int(_views), int(_comments), str(_video_privacy_status), str(_video_url),
                     str(_category), str(_embedding_allowed), str(_ratings_allowed),
                     str(_comments_allowed), str(_claim_origin), str(_content_type), str(_upload_source),
                     str(_claimed_by_this_owner), str(_claimed_by_another_owner), str(_other_owners_claiming), str(_offweb_syndicatable),
                     str(_claim_id), str(_asset_id), str(_custom_id), str(_effective_policy), str(_third_party_video_id),
                     str(_third_party_ads_enabled),str(_display_ads_enabled), str(_sponsored_cards_enabled), str(_overlay_ads_enabled),
                     str(_nonskippable_video_ads_enabled),str(_long_nonskippable_video_ads_enabled), str(_skippable_video_ads_enabled), str(_prerolls_enabled),
                     str(_postrolls_enabled),str(_isrc), str(_eidr)))
        if i_row != 0 and i_row % 1500 == 0:
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%",
                  "------- %s seconds -------" % round((time.time() - start_time), 2))
            time.sleep(5)
            _arr = []
    if len(_arr) > 0:
        errors = client.insert_rows(table, _arr)
        assert errors == []
        print("------- Done! 100% uploaded. -------")
        time.sleep(5)
        _arr = []

def daily_reports(content_owner,_content_owner,_table,_jobid):
    # TODO: DAILY REPORTS
    lastest_date_BQ = get_latest_date_func(content_owner=_content_owner, table=_table)
    print("--------------------------" + _table + "--------------------------")
    _retrieve_reports = retrieve_reports(youtube_reporting, lastest_date_BQ=lastest_date_BQ,
                                         jobId=_jobid[str(_table)],
                                         onBehalfOfContentOwner=content_owners[str(content_owner)])
    if bool(_retrieve_reports) == True:
        print("----- The newest_date_on_BQ: {0} but {1} on API ".format(lastest_date_BQ, max(_retrieve_reports.keys())))
        print("---------- ", len(_retrieve_reports), " reports need to retrieve ----------")
        for date_retreive, value_retrieve in _retrieve_reports.items():
            print("-------- " + _table + " Updating date: ", date_retreive, " -------------------")
            report_url = value_retrieve[1]
            download_report(youtube_reporting, report_url,
                            _content_owner + "_" + _table + "_" + str(date_retreive).split(" ")[0] + ".txt")
            for _file in check_files(FOLDER_PATH):
                if "p_content_owner_basic_a3" in _file and _file == _content_owner + "_" + _table + "_" + \
                        str(date_retreive).split(" ")[0] + ".txt":
                    try:
                        print("upload to: ",_content_owner,_table,str(date_retreive).split(" ")[0])
                        p_content_owner_basic_a3(file_name=_file,dataset_id=_content_owner,
                                                          _content_owner=_content_owner,_table=_table,
                                                          _date=str(date_retreive).split(" ")[0])
                        print("{0} Inserted successfully".format(_table))
                        check_upload(file_name=_file,content_owner=_content_owner,table=_table,_date=str(date_retreive).split(" ")[0])
                    except (TypeError) as e:
                        pass
                elif "p_content_owner_estimated_revenue_a1" in _file and _file == _content_owner + "_" + _table + "_" + \
                        str(date_retreive).split(" ")[0] + ".txt":
                    try:
                        print("upload to: ",_content_owner,_table,str(date_retreive).split(" ")[0])
                        p_content_owner_estimated_revenue_a1(file_name=_file,dataset_id=_content_owner,
                                                          _content_owner=_content_owner,_table=_table,
                                                          _date=str(date_retreive).split(" ")[0])
                        print("{0} Inserted successfully".format(_table))
                        check_upload(file_name=_file,content_owner=_content_owner,table=_table,_date=str(date_retreive).split(" ")[0])
                    except (TypeError) as e:
                        pass
                elif "p_content_owner_video_metadata_a2" in _file and _file == _content_owner + "_" + _table + "_" + \
                        str(date_retreive).split(" ")[0] + ".txt":
                    try:
                        print("upload to: ",_content_owner,_table,str(date_retreive).split(" ")[0])
                        p_content_owner_video_metadata_a2(file_name=_file,dataset_id=_content_owner,
                                                          _content_owner=_content_owner,_table=_table,
                                                          _date=str(date_retreive).split(" ")[0])
                        print("{0} Inserted successfully".format(_table))
                        check_upload(file_name=_file,content_owner=_content_owner,table=_table,_date=str(date_retreive).split(" ")[0])
                    except (TypeError) as e:
                        pass
                else:
                    pass
        print("---------------------" + _content_owner + " - " + _table + " DONE! ---------------------")
    else:
        print("-----" + _content_owner + "-" + _table + " is up to date! " + "-----" )

def monthly_reports(_content_owner,_table,_jobid):
    print("----------------- Checking Monthly Report --------------------")
    newest_month_BG = get_lastest_month_func(content_owner=_content_owner, table=_table)
    print(newest_month_BG)
    print("--------------------------" + _table + "--------------------------")
    _retrieve_reports_monthly = retrieve_reports(youtube_reporting, lastest_date_BQ=newest_month_BG,
                                                 jobId=_jobid[str(_table)],
                                                 onBehalfOfContentOwner=content_owners[str(content_owner)])
    if bool(_retrieve_reports_monthly) == True:
        print("----- The newest_date_on_BQ: {0} but {1} on API ".format(newest_month_BG,
                                                                        max(_retrieve_reports_monthly.keys())))
        print("---------- ", len(_retrieve_reports_monthly), " reports need to retrieve ----------")
        for date_retreive, value_retrieve in _retrieve_reports_monthly.items():
            print("-------- " + _table + " Updating date: ", date_retreive, " -------------------")
            report_url = value_retrieve[1]
            download_report(youtube_reporting, report_url, content_owner+"_"+_table+"_"+str(date_retreive).split(" ")[0]+".txt")
            for _file in check_files(FOLDER_PATH):
                if "p_content_owner_ad_revenue_raw_a1" in _file and _file == _content_owner + "_" + _table + "_" + \
                        str(date_retreive).split(" ")[0] + ".txt":
                    try:
                        print("upload to: ",_content_owner,_table,str(date_retreive).split(" ")[0])
                        p_content_owner_ad_revenue_raw_a1(file_name=_file,dataset_id=_content_owner,_content_owner=_content_owner,_table=_table,_date=str(date_retreive).split(" ")[0] )
                        print("{0} Inserted successfully".format(_table))
                        check_upload(file_name=_file,content_owner=_content_owner,table=_table,_date=str(date_retreive).split(" ")[0])
                    except (TypeError) as e:
                        pass
                else:
                    pass
        print("---------------------" + _content_owner + " - " + _table + " DONE! ---------------------")
    else:
        print("-----" + _content_owner + " - " + _table + " is up to date!")

if __name__ == '__main__':
    #    Music: UPtu1ivBRvjYBGoz0m0Dfg
    #    Kids: ra8f1uKU-uu9osqlt3jb5g
    #    Ent: ncwbWh1Q1LCsMAeryRBocQ
    #    Aff: 9C1hXNjkMN_2GO7ARpaplw
    #    Music-TH: iTE9_S8Uo42n3JzGAiht1w
    #    Ent-TH: RPppPCMzH4DTihKfvR8EeA
    #    Aff-TH: xtl5pd5oPxbhI0dI0Qpkyg
    FOLDER_PATH = "C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\"
    youtube_reporting = get_authenticated_service()
    # "yt_music":"UPtu1ivBRvjYBGoz0m0Dfg","yt_kids":"ra8f1uKU-uu9osqlt3jb5g","yt_entertainment":"ncwbWh1Q1LCsMAeryRBocQ","yt_affiliate":"9C1hXNjkMN_2GO7ARpaplw","yt_th_music":"iTE9_S8Uo42n3JzGAiht1w","yt_th_entertainment":"RPppPCMzH4DTihKfvR8EeA", "yt_th_affiliate":"xtl5pd5oPxbhI0dI0Qpkyg"
    content_owners = {"yt_kids":"ra8f1uKU-uu9osqlt3jb5g"}
    yt_music = {
                    # "p_content_owner_basic_a3_yt_music": "04cda439-5fd1-4722-82cf-75bb311f0bdc",
                    # "p_content_owner_estimated_revenue_a1_yt_music": "05122e5b-94b0-4368-945e-451b23451cae",
                    "p_content_owner_video_metadata_a2_yt_music": "dad11517-31c9-4af5-ad62-d4c986743aec"
                       # "p_content_owner_ad_revenue_raw_a1_yt_music":"58411f62-d342-4b81-ab7e-5fe4c479808f"
                }

    yt_kids = {"p_content_owner_basic_a3_yt_kids": "1d2b93e9-7de3-4e88-876a-831d3d0e1e83",
                   "p_content_owner_estimated_revenue_a1_yt_kids": "0ef9cb40-039c-4c75-aa84-b96e60b0029d",
                   "p_content_owner_video_metadata_a2_yt_kids": "69749123-641b-43b2-a4f1-30494c6740d3"
                       # "p_content_owner_ad_revenue_raw_a1_yt_kids":"7ab6eeee-2cb4-4668-a3be-03ed125b7eb4"
               }

    yt_entertainment = {"p_content_owner_basic_a3_yt_entertainment": "c071a36c-a9ef-4ef7-8624-4f7fbeac0702",
                  "p_content_owner_estimated_revenue_a1_yt_entertainment": "9955d3f3-8648-4334-8c9e-5722f6999282",
                  "p_content_owner_video_metadata_a2_yt_entertainment": "8c38616b-358c-401b-8d4f-e9791cc50281"
                       # "p_content_owner_ad_revenue_raw_a1_yt_entertainment":"101b91ce-92a1-4ded-a2db-0de96af565e0"
                        }

    yt_affiliate = {"p_content_owner_basic_a3_yt_affiliate": "4f47209b-07f2-4f2b-a709-7cc5e414c0cf",
                  "p_content_owner_estimated_revenue_a1_yt_affiliate": "6fe1250d-6851-4468-aba1-9f6adff24a81",
                  "p_content_owner_video_metadata_a2_yt_affiliate": "d33a4a31-fb27-46ea-b1e7-968cb2a38d99"
                       # "p_content_owner_ad_revenue_raw_a1_yt_affiliate":"d8fc0ba0-db14-46e0-adf8-560c956fbfea"
                    }

    yt_th_music = {
                       "p_content_owner_basic_a3_yt_th_music": "70f232e5-4c75-4613-8461-1c615a51269c",
                       "p_content_owner_estimated_revenue_a1_yt_th_music": "406770b1-c4e8-4a25-8cd4-89da792a7211",
                       "p_content_owner_video_metadata_a2_yt_th_music": "2f63b5da-7006-440e-9a95-69e6b5f4878d"
                       # "p_content_owner_ad_revenue_raw_a1_yt_th_music":"d16b91f2-5885-4fbb-9ee6-3c3994decc37"
                   }

    yt_th_entertainment = {"p_content_owner_basic_a3_yt_th_entertainment": "45ffe8a1-9213-44d4-8d06-16102f163801",
                     "p_content_owner_estimated_revenue_a1_yt_th_entertainment": "9d8b729a-4d0f-46c4-a17e-b83c806b5621",
                     "p_content_owner_video_metadata_a2_yt_th_entertainment": "0764aba7-687c-4ab0-a462-11d3c1743b53"
                     #   "p_content_owner_ad_revenue_raw_a1_yt_th_entertainment":"10a19692-9ad8-45ef-9ceb-cdd914337818"
                    }

    yt_th_affiliate = {
                        "p_content_owner_basic_a3_yt_th_affiliate": "bab9bac2-4c5a-4cb5-b168-ba58a659fafe",
                        "p_content_owner_estimated_revenue_a1_yt_th_affiliate": "02d59ec8-ea7e-46c1-9ccc-9a48a7250575",
                       "p_content_owner_video_metadata_a2_yt_th_affiliate": "0b85e9c6-83b1-44fa-9ae3-0fee70f26259"
                       # "p_content_owner_ad_revenue_raw_a1_yt_th_affiliate":"9f2c221b-3ec2-419f-9e4c-d8aa7494b2db"
    }

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pops-dab1c699446f.json"
    client = bigquery.Client()
    try:
        for content_owner in content_owners:
            print("------------------------Working on {} CMS------------------------".format(content_owner))
            if content_owner == "yt_music":
                for table in yt_music:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid=yt_music)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner="1969",_table=table,_jobid=yt_music)
            elif content_owner == "yt_kids":
                for table in yt_kids:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid=yt_kids)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner="1969",_table=table,_jobid=yt_kids)
            elif content_owner == "yt_entertainment":
                for table in yt_entertainment:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid=yt_entertainment)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner="1969",_table=table,_jobid=yt_entertainment)
            elif content_owner == "yt_affiliate":
                for table in yt_affiliate:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid= yt_affiliate)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner=content_owner,_table=table,_jobid= yt_affiliate)
            elif content_owner == "yt_th_music":
                for table in yt_th_music:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid= yt_th_music)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner="1969",_table=table,_jobid= yt_th_music)
            elif content_owner == "yt_th_entertainment":
                for table in yt_th_entertainment:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid= yt_th_entertainment)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner="1969",_table=table,_jobid= yt_th_entertainment)
            elif content_owner == "yt_th_affiliate":
                for table in yt_th_affiliate:
                    if "owner_ad_revenue_raw" not in str(table):
                        # TODO: DAILY REPORTS
                        daily_reports(content_owner=content_owner,_content_owner="1969",_table=table,_jobid= yt_th_affiliate)
                    else:
                        # TODO: MONTHLY REPORTS
                        monthly_reports(_content_owner="1969",_table=table,_jobid=yt_th_affiliate)
            else:
                print("Impossible case. Does not match content_owner.")
    except (HttpError,socket.timeout) as e:
        print('An HTTP error %d occurred:\n %s' % (e.resp.status, e.content))
# type: ignore
