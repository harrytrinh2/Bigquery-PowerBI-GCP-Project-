#!/usr/bin/python
# -*- coding: utf-8 -*-


import argparse
import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
import pandas as pd
import os,glob
from google.cloud import bigquery
import time,datetime
from datetime import timedelta
import numpy as np
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from io import FileIO
import socket

CLIENT_SECRETS_FILE = "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json"

SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']
API_SERVICE_NAME = 'youtubereporting'
API_VERSION = 'v1'


# Authorize the request and store authorization credentials.
def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


# Remove keyword arguments that are not set.
def remove_empty_kwargs(**kwargs):
    good_kwargs = {}
    if kwargs is not None:
        for key, value in kwargs.items():
            if value:
                good_kwargs[key] = value
    return good_kwargs

# Call the YouTube Reporting API's jobs.list method to retrieve reporting jobs.
def list_reporting_jobs(youtube_reporting, **kwargs):
    # Only include the onBehalfOfContentOwner keyword argument if the user
    # set a value for the --content_owner argument.
    kwargs = remove_empty_kwargs(**kwargs)

    # Retrieve the reporting jobs for the user (or content owner).
    results = youtube_reporting.jobs().list(**kwargs).execute()

    if 'jobs' in results and results['jobs']:
        jobs = results['jobs']
        for job in jobs:
            print('Reporting job id: %s\n name: %s\n for reporting type: %s\n'
                  % (job['id'], job['name'], job['reportTypeId']))
    else:
        print('No jobs found')
        return False

    return True


# Call the YouTube Reporting API's reports.list method to retrieve reports created by a job.
def retrieve_reports(youtube_reporting,LATEST_date,LATEST_date_next, **kwargs):
    # Only include the onBehalfOfContentOwner keyword argument if the user
    # set a value for the --content_owner argument.
    kwargs = remove_empty_kwargs(**kwargs)

    # Retrieve available reports for the selected job.
    results = youtube_reporting.jobs().reports().list(**kwargs).execute()

    if 'reports' in results and results['reports']:
        reports = results['reports']
        jobId = ''
        startTime = ''
        endTime = ''
        downloadUrl = ''
        for id in reports:
            if id["startTime"] == str(LATEST_date)+"T08:00:00Z" and id["endTime"] == str(LATEST_date_next) + "T08:00:00Z":
                jobId = id["jobId"]
                startTime = id["startTime"]
                endTime = id["endTime"]
                downloadUrl = id["downloadUrl"]
    return (downloadUrl)

# Call the YouTube Reporting API's media.download method to download the report.
def download_report(youtube_reporting, report_url, local_file):
    request = youtube_reporting.media().download(
        resourceName=' '
    )
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

# Prompt the user to select a job and return the specified ID.
def get_job_id_from_user():
    job_id = input('Please enter the job id for the report retrieval: ')
    print('You chose "%s" as the job Id for the report retrieval.' % job_id)
    return job_id

# Prompt the user to select a report URL and return the specified URL.
def get_report_url_from_user(report_url):
    # report_url = input('Please enter the report URL to download: ')
    print('You chose "%s" to download.' % report_url)
    return report_url

def get_latest_date(query):
    query_job = client.query(query)
    rows = query_job.result()
    for i_row in rows:
        _LATEST_DATE = i_row.get('NEWEST_DAY')
        return _LATEST_DATE

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
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%")
            print("--- %s seconds ---" % (time.time() - start_time))
            time.sleep(5)
            _arr = []

def owner_estimated_revenue_a1(file_name,table_id, dataset_id ):
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
        _arr.append((_date,_channel_id,_video_id,_claimed_status,_uploader_type,_country_code,_estimated_partner_revenue,
                     _estimated_partner_ad_revenue,_estimated_partner_ad_auction_revenue,_estimated_partner_ad_reserved_revenue,
                     _estimated_youtube_ad_revenue,_estimated_monetized_playbacks,_estimated_playback_based_cpm,
                     _ad_impressions,_estimated_cpm,_estimated_partner_red_revenue,_estimated_partner_transaction_revenue))
        if i_row != 0 and i_row % 1500 == 0:
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%")
            print("--- %s seconds ---" % (time.time() - start_time))
            time.sleep(5)
            _arr = []
def owner_ad_revenue_summary_a1(file_name,table_id, dataset_id ):
    data = pd.read_table(file_name,sep=",")
    data = data.replace(np.nan,'',regex=True)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    start_time = time.time()
    _arr = []
    for i_row in range(len(data)):
        _adjustment_type = data['adjustment_type'][i_row]
        _date = data['date'][i_row]
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
        _youtube_revenue_split_partner_sold_youtube_served = data['youtube_revenue_split_partner_sold_youtube_served'][i_row]
        _youtube_revenue_split_partner_sold_partner_served = data['youtube_revenue_split_partner_sold_partner_served'][i_row]
        _youtube_revenue_split = data['youtube_revenue_split'][i_row]
        _partner_revenue_auction = data['partner_revenue_auction'][i_row]
        _partner_revenue_reserved = data['partner_revenue_reserved'][i_row]
        _partner_revenue_partner_sold_youtube_served = data['partner_revenue_partner_sold_youtube_served'][i_row]
        _partner_revenue_partner_sold_partner_served = data['partner_revenue_partner_sold_partner_served'][i_row]
        _partner_revenue = data['partner_revenue'][i_row]

        _arr.append((_adjustment_type,_date,_video_id,_video_title,_video_duration_sec,_category,_channel_id,
                     _uploader,_channel_display_name,_content_type,_policy,
                     _owned_views,_youtube_revenue_split_auction,_youtube_revenue_split_reserved,
                     _youtube_revenue_split_partner_sold_youtube_served,_youtube_revenue_split_partner_sold_partner_served,
                     _youtube_revenue_split,_partner_revenue_auction,_partner_revenue_reserved,
                     _partner_revenue_partner_sold_youtube_served,_partner_revenue_partner_sold_partner_served,_partner_revenue))
        if i_row != 0 and i_row % 1500 == 0:
            errors = client.insert_rows(table, _arr)
            assert errors == []
            print(round((float(int(i_row) / float(len(data))) * 100), 2), "%")
            print("--- %s seconds ---" % (time.time() - start_time))
            time.sleep(5)
            _arr = []
def check_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file
if __name__ == '__main__':
    youtube_reporting = get_authenticated_service()
    content_owner = {"MUSIC":"UPtu1ivBRvjYBGoz0m0Dfg","KIDS":"ra8f1uKU-uu9osqlt3jb5g","ENT":"ncwbWh1Q1LCsMAeryRBocQ","AFFILIATE":"9C1hXNjkMN_2GO7ARpaplw","MUSIC-TH":"iTE9_S8Uo42n3JzGAiht1w","ENT-TH":"RPppPCMzH4DTihKfvR8EeA","AFF-TH":"xtl5pd5oPxbhI0dI0Qpkyg"}
    job_id_Music = {"Owner_basic": "04cda439-5fd1-4722-82cf-75bb311f0bdc",
                    "Estimated_revenue": "05122e5b-94b0-4368-945e-451b23451cae"}

    job_id_Kids = {"Owner_basic": "1d2b93e9-7de3-4e88-876a-831d3d0e1e83",
                   "Estimated_revenue": "0ef9cb40-039c-4c75-aa84-b96e60b0029d"}

    job_id_Ent = {"Owner_basic": "c071a36c-a9ef-4ef7-8624-4f7fbeac0702",
                  "Estimated_revenue": "9955d3f3-8648-4334-8c9e-5722f6999282"}

    job_id_Aff = {"Owner_basic": "4f47209b-07f2-4f2b-a709-7cc5e414c0cf",
                  "Estimated_revenue": "6fe1250d-6851-4468-aba1-9f6adff24a81"}

    job_id_Music_TH = {"Owner_basic": "70f232e5-4c75-4613-8461-1c615a51269c",
                       "Estimated_revenue": "406770b1-c4e8-4a25-8cd4-89da792a7211"}

    job_id_Ent_TH = {"Owner_basic": "45ffe8a1-9213-44d4-8d06-16102f163801",
                     "Estimated_revenue": "9d8b729a-4d0f-46c4-a17e-b83c806b5621"}

    job_id_Aff_TH = {"Owner_basic": "bab9bac2-4c5a-4cb5-b168-ba58a659fafe",
                     "Estimated_revenue": "02d59ec8-ea7e-46c1-9ccc-9a48a7250575"}
    dataset_id = ['yt_music', 'yt_kids', 'yt_entertainment', 'yt_affiliate', 'yt_th_music', 'yt_th_entertainment','yt_th_affiliate']

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pops-dab1c699446f.json"
    client = bigquery.Client()
    LATEST_DATE = get_latest_date('''SELECT DATE(MAX(_PARTITIONTIME)) AS `NEWEST_DAY`
                       FROM `pops-204909.yt_kids.p_content_owner_basic_a3_yt_kids`''')
    LATEST_DATE_NEXT = LATEST_DATE + timedelta(days=1)
    print(LATEST_DATE,LATEST_DATE_NEXT)
    check_update = False
    for file in check_files("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\"):
        if str(LATEST_DATE) not in str(file):
            continue
        else:
            check_update = True
            break
    if check_update == True:
        print("Data has been updated!")
    else:
        try:
            for content_owner_num in content_owner:
                print("------------------------Working on {} CMS------------------------".format(content_owner_num))
                if content_owner_num == "MUSIC":
                    for job_id_num in job_id_Music:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT, jobId=job_id_Music[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                elif content_owner_num == "KIDS":
                    for job_id_num in job_id_Kids:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT, jobId=job_id_Kids[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                elif content_owner_num == "ENT":
                    for job_id_num in job_id_Ent:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT, jobId=job_id_Ent[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                elif content_owner_num == "AFFILIATE":
                    for job_id_num in job_id_Aff:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT, jobId=job_id_Aff[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                elif content_owner_num == "MUSIC-TH":
                    for job_id_num in job_id_Music_TH:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT,
                                                jobId=job_id_Music_TH[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                elif content_owner_num == "ENT-TH":
                    for job_id_num in job_id_Ent_TH:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT,
                                                jobId=job_id_Ent_TH[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                elif content_owner_num == "AFF-TH":
                    for job_id_num in job_id_Aff_TH:
                        print("--------------------------" + job_id_num + "--------------------------")
                        report_url = get_report_url_from_user(
                            retrieve_reports(youtube_reporting, LATEST_date=LATEST_DATE,
                                                LATEST_date_next=LATEST_DATE_NEXT,
                                                jobId=job_id_Aff_TH[str(job_id_num)],
                                                onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url,
                                        content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE) + ".txt")
                        print(
                            "---------------------" + content_owner_num + " - " + job_id_num + " DONE! ---------------------")
                        for _file in glob.glob(
                                os.path.join("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\", "*.txt")):
                            if content_owner_num + "_" + "Owner_basic" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                content_owner_basic_a3(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                            elif content_owner_num + "_" + "Estimated_revenue" + "_" + str(LATEST_DATE) is _file:
                                print("Importing " + content_owner_num + "_" + job_id_num + "_" + str(LATEST_DATE))
                                owner_estimated_revenue_a1(file_name=_file, table_id="", dataset_id="yt_music")
                                print("Imported successfully " + content_owner_num + "_" + job_id_num + "_" + str(
                                    LATEST_DATE))
                else:
                    print("Does not match any tables :(")
        except (HttpError, socket.timeout) as e:
            print('An HTTP error %d occurred:\n %s' % (e.resp.status, e.content))
# type: ignore