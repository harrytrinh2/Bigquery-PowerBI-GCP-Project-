#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
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
      print ('Reporting job id: %s\n name: %s\n for reporting type: %s\n' % (job['id'], job['name'], job['reportTypeId']))
  else:
    print ('No jobs found')
    return False
  return True

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

def get_latest_date_func(content_owner,table):
    query_job = client.query('''SELECT DATE(MAX(_PARTITIONTIME)) AS `lastest_date_BQ`
                                        FROM `pops-204909.''' + content_owner + "." + table + "`")
    rows = query_job.result()
    for i_row in rows:
        _lastest_date_BQ = i_row.get('lastest_date_BQ')
        return datetime(_lastest_date_BQ.year,_lastest_date_BQ.month,_lastest_date_BQ.day)
def check_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

if __name__ == '__main__':
    #    Music: UPtu1ivBRvjYBGoz0m0Dfg
    #    Kids: ra8f1uKU-uu9osqlt3jb5g
    #    Ent: ncwbWh1Q1LCsMAeryRBocQ
    #    Aff: 9C1hXNjkMN_2GO7ARpaplw
    #    Music-TH: iTE9_S8Uo42n3JzGAiht1w
    #    Ent-TH: RPppPCMzH4DTihKfvR8EeA
    #    Aff-TH: xtl5pd5oPxbhI0dI0Qpkyg
    youtube_reporting = get_authenticated_service()
    # "yt_music":"UPtu1ivBRvjYBGoz0m0Dfg","yt_kids":"ra8f1uKU-uu9osqlt3jb5g","yt_entertainment":"ncwbWh1Q1LCsMAeryRBocQ","yt_affiliate":"9C1hXNjkMN_2GO7ARpaplw","yt_th_music":"iTE9_S8Uo42n3JzGAiht1w","yt_th_entertainment":"RPppPCMzH4DTihKfvR8EeA",
    content_owners = {"yt_th_affiliate":"xtl5pd5oPxbhI0dI0Qpkyg"}
    yt_music = {"p_content_owner_basic_a3_yt_music": "04cda439-5fd1-4722-82cf-75bb311f0bdc",
                    "p_content_owner_estimated_revenue_a1_yt_music": "05122e5b-94b0-4368-945e-451b23451cae",
                    "p_content_owner_video_metadata_a2_yt_music": "dad11517-31c9-4af5-ad62-d4c986743aec"}

    yt_kids = {"p_content_owner_basic_a3_yt_kids": "1d2b93e9-7de3-4e88-876a-831d3d0e1e83",
                   "p_content_owner_estimated_revenue_a1_yt_kids": "0ef9cb40-039c-4c75-aa84-b96e60b0029d",
                   "p_content_owner_video_metadata_a2_yt_kids": "69749123-641b-43b2-a4f1-30494c6740d3"}

    yt_entertainment = {"p_content_owner_basic_a3_yt_entertainment": "c071a36c-a9ef-4ef7-8624-4f7fbeac0702",
                  "p_content_owner_estimated_revenue_a1_yt_entertainment": "9955d3f3-8648-4334-8c9e-5722f6999282",
                  "p_content_owner_video_metadata_a2_yt_entertainment": "8c38616b-358c-401b-8d4f-e9791cc50281"}

    yt_affiliate = {"p_content_owner_basic_a3_yt_affiliate": "4f47209b-07f2-4f2b-a709-7cc5e414c0cf",
                  "p_content_owner_estimated_revenue_a1_yt_affiliate": "6fe1250d-6851-4468-aba1-9f6adff24a81",
                  "p_content_owner_video_metadata_a2_yt_affiliate": "d33a4a31-fb27-46ea-b1e7-968cb2a38d99"}

    yt_th_music = {"p_content_owner_basic_a3_yt_th_music": "70f232e5-4c75-4613-8461-1c615a51269c",
                       "p_content_owner_estimated_revenue_a1_yt_th_music": "406770b1-c4e8-4a25-8cd4-89da792a7211",
                       "p_content_owner_video_metadata_a2_yt_th_music": "2f63b5da-7006-440e-9a95-69e6b5f4878d"}

    yt_th_entertainment = {"p_content_owner_basic_a3_yt_th_entertainment": "45ffe8a1-9213-44d4-8d06-16102f163801",
                     "p_content_owner_estimated_revenue_a1_yt_th_entertainment": "9d8b729a-4d0f-46c4-a17e-b83c806b5621",
                     "p_content_owner_video_metadata_a2_yt_th_entertainment": "0764aba7-687c-4ab0-a462-11d3c1743b53"}

    yt_th_affiliate = {"p_content_owner_basic_a3_yt_th_affiliate": "bab9bac2-4c5a-4cb5-b168-ba58a659fafe",
                     "p_content_owner_estimated_revenue_a1_yt_th_affiliate": "02d59ec8-ea7e-46c1-9ccc-9a48a7250575",
                     "p_content_owner_video_metadata_a2_yt_th_affiliate": "0b85e9c6-83b1-44fa-9ae3-0fee70f26259"}

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pops-dab1c699446f.json"
    client = bigquery.Client()
    try:
        for content_owner in content_owners:
            print("------------------------Working on {} CMS------------------------".format(content_owner))
            if content_owner == "yt_music":
                for table in yt_music:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_music[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")
            elif content_owner == "yt_kids":
                for table in yt_kids:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_kids[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")
            elif content_owner == "yt_entertainment":
                for table in yt_entertainment:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)                    
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_entertainment[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")

            elif content_owner == "yt_affiliate":
                for table in yt_affiliate:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_affiliate[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")
                
            elif content_owner == "yt_th_music":
                for table in yt_th_music:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)                    
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_th_music[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")
                
            elif content_owner == "yt_th_entertainment":
                for table in yt_th_entertainment:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)                    
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_th_entertainment[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")
                
            elif content_owner == "yt_th_affiliate":
                for table in yt_th_affiliate:
                    lastest_date_BQ  = get_latest_date_func(content_owner,table)                    
                    print("--------------------------"+table+"--------------------------")
                    _retrieve_reports = retrieve_reports(youtube_reporting,lastest_date_BQ=lastest_date_BQ,jobId=yt_th_affiliate[str(table)],onBehalfOfContentOwner=content_owners[str(content_owner)])
                    print("----- The newest_date_on_BQ: {0} and {1} on API ".format(lastest_date_BQ,max(_retrieve_reports.keys())))
                    print("---------- ",len(_retrieve_reports)," reports need to retrieve ----------")
                    for date_retreive,value_retrieve in _retrieve_reports.items():
                        print("-------- "+table+" Updating date: ",date_retreive," -------------------")
                        report_url = value_retrieve[1]
                        download_report(youtube_reporting, report_url, content_owner+"_"+table+"_"+str(date_retreive)+".txt")
                    print("---------------------" + content_owner + " - " +table + " DONE! ---------------------")
            else:
                print("Impossible case. Does not match content_owner.")
    except (HttpError,socket.timeout) as e:
        print('An HTTP error %d occurred:\n %s' % (e.resp.status, e.content))
# type: ignore
