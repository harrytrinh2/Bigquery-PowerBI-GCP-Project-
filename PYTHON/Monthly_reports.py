#!/usr/bin/python
# -*- coding: utf-8 -*-
###
#
# This script retrieves YouTube Reporting API reports. Use cases:
# 1. If you specify a report URL, the script downloads that report.
# 2. Otherwise, if you specify a job ID, the script retrieves a list of
#    available reports for that job and prompts you to select a report.
#    Then it retrieves that report as in case 1.
# 3. Otherwise, the list retrieves a list of jobs for the user or,
#    if specified, the content owner that the user is acting on behalf of.
#    Then it prompts the user to select a job, and then executes case 2 and
#    then case 1.
# Usage examples:
# python retrieve_reports.py --content_owner_id=<CONTENT_OWNER_ID> --local_file=<LOCAL_FILE>
# python retrieve_reports.py --content_owner_id=<CONTENT_OWNER_ID> --job_id=<JOB_ID> --local_file=<LOCAL_FILE>
# python retrieve_reports.py --content_owner_id=<CONTENT_OWNER_ID> --report_url=<REPORT_URL> --local_file=<LOCAL_FILE>
#
###

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
      print ('Reporting job id: %s\n name: %s\n for reporting type: %s\n'
        % (job['id'], job['name'], job['reportTypeId']))
  else:
    print ('No jobs found')
    return False

  return True


# Call the YouTube Reporting API's reports.list method to retrieve reports created by a job.
def retrieve_reports(youtube_reporting,LATEST_DATE,LATEST_DATE_next, **kwargs):
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
            if id["startTime"] == str(LATEST_DATE)+"T08:00:00Z" and id["endTime"] == str(LATEST_DATE_next) + "T08:00:00Z":
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
        _LATEST_DATE = i_row.get('LATEST_DATE')
        return _LATEST_DATE
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
    content_owner = {"MUSIC":"UPtu1ivBRvjYBGoz0m0Dfg","KIDS":"ra8f1uKU-uu9osqlt3jb5g","ENT":"ncwbWh1Q1LCsMAeryRBocQ","AFFILIATE":"9C1hXNjkMN_2GO7ARpaplw","MUSIC-TH":"iTE9_S8Uo42n3JzGAiht1w","ENT-TH":"RPppPCMzH4DTihKfvR8EeA","AFF-TH":"xtl5pd5oPxbhI0dI0Qpkyg"}
    job_id_Music = {"content_owner_ad_revenue_raw_a1": "58411f62-d342-4b81-ab7e-5fe4c479808f",
                    "content_owner_video_metadata_a2": "dad11517-31c9-4af5-ad62-d4c986743aec"}

    job_id_Kids = {"content_owner_ad_revenue_raw_a1": "1d2b93e9-7de3-4e88-876a-831d3d0e1e83",
                   "content_owner_video_metadata_a2": "0ef9cb40-039c-4c75-aa84-b96e60b0029d"}

    job_id_Ent = {"content_owner_ad_revenue_raw_a1": "c071a36c-a9ef-4ef7-8624-4f7fbeac0702",
                  "content_owner_video_metadata_a2": "9955d3f3-8648-4334-8c9e-5722f6999282"}

    job_id_Aff = {"content_owner_ad_revenue_raw_a1": "4f47209b-07f2-4f2b-a709-7cc5e414c0cf",
                  "content_owner_video_metadata_a2": "6fe1250d-6851-4468-aba1-9f6adff24a81"}

    job_id_Music_TH = {"content_owner_ad_revenue_raw_a1": "70f232e5-4c75-4613-8461-1c615a51269c",
                       "content_owner_video_metadata_a2": "406770b1-c4e8-4a25-8cd4-89da792a7211"}

    job_id_Ent_TH = {"content_owner_ad_revenue_raw_a1": "45ffe8a1-9213-44d4-8d06-16102f163801",
                     "content_owner_video_metadata_a2": "9d8b729a-4d0f-46c4-a17e-b83c806b5621"}

    job_id_Aff_TH = {"content_owner_ad_revenue_raw_a1": "bab9bac2-4c5a-4cb5-b168-ba58a659fafe",
                     "content_owner_video_metadata_a2": "02d59ec8-ea7e-46c1-9ccc-9a48a7250575"}
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pops-dab1c699446f.json"
    client = bigquery.Client()
    LATEST_DATE  = get_latest_date('''SELECT DATE(MAX(_PARTITIONTIME)) AS `LATEST_DATE`
                       FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`''')
    print()
    LATEST_DATE_NEXT = LATEST_DATE + timedelta(days=1)
    print("LATEST_DATE: " ,LATEST_DATE)
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
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Music[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "KIDS":
                    for job_id_num in job_id_Kids:
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Kids[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "ENT":
                    for job_id_num in job_id_Ent:
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Ent[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "AFFILIATE":
                    for job_id_num in job_id_Aff:
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Aff[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "MUSIC-TH":
                    for job_id_num in job_id_Music_TH:
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Music_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "ENT-TH":
                    for job_id_num in job_id_Ent_TH:
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Ent_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "AFF-TH":
                    for job_id_num in job_id_Aff_TH:
                        print("--------------------------"+job_id_num+"--------------------------")
                        report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,LATEST_DATE=LATEST_DATE,LATEST_DATE_next=LATEST_DATE_NEXT,jobId=job_id_Aff_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(LATEST_DATE)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                else:
                    print("Does not match content_owner_num")
        except (HttpError,socket.timeout) as e:
            print('An HTTP error %d occurred:\n %s' % (e.resp.status, e.content))
# type: ignore
