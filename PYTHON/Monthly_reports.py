#!/usr/bin/python
# -*- coding: utf-8 -*-

import calendar
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
import datetime

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
def retrieve_reports(youtube_reporting,newest_month_BG,newest_month_BG_NEXT, **kwargs):
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
            if id["startTime"] == str(newest_month_BG)+"T08:00:00Z" and id["endTime"] == str(newest_month_BG_NEXT) + "T08:00:00Z":
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

def get_lastest_month_func(query):
    query_job = client.query(query)
    rows = query_job.result()
    for i_row in rows:
        _newest_month_BG = i_row.get('newest_month_BG')
        return _newest_month_BG
def check_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file
def add_months(sourcedate,months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    return datetime.date(year,month,day)

if __name__ == '__main__':
    #    Music: UPtu1ivBRvjYBGoz0m0Dfg
    #    Kids: ra8f1uKU-uu9osqlt3jb5g
    #    Ent: ncwbWh1Q1LCsMAeryRBocQ
    #    Aff: 9C1hXNjkMN_2GO7ARpaplw
    #    Music-TH: iTE9_S8Uo42n3JzGAiht1w
    #    Ent-TH: RPppPCMzH4DTihKfvR8EeA
    #    Aff-TH: xtl5pd5oPxbhI0dI0Qpkyg
    # "MUSIC":"UPtu1ivBRvjYBGoz0m0Dfg","KIDS":"ra8f1uKU-uu9osqlt3jb5g",
    # ,"AFFILIATE":"9C1hXNjkMN_2GO7ARpaplw","MUSIC-TH":"iTE9_S8Uo42n3JzGAiht1w","ENT-TH":"RPppPCMzH4DTihKfvR8EeA","AFF-TH":"xtl5pd5oPxbhI0dI0Qpkyg"
    youtube_reporting = get_authenticated_service()
    content_owner = {"ENT":"ncwbWh1Q1LCsMAeryRBocQ"}
    job_id_Music = {"content_owner_ad_revenue_raw_a1": "58411f62-d342-4b81-ab7e-5fe4c479808f"}

    job_id_Kids = {"content_owner_ad_revenue_raw_a1": "7ab6eeee-2cb4-4668-a3be-03ed125b7eb4"}

    job_id_Ent = {"content_owner_ad_revenue_raw_a1": "101b91ce-92a1-4ded-a2db-0de96af565e0"}

    job_id_Aff = {"content_owner_ad_revenue_raw_a1": "d8fc0ba0-db14-46e0-adf8-560c956fbfea"}

    job_id_Music_TH = {"content_owner_ad_revenue_raw_a1": "d16b91f2-5885-4fbb-9ee6-3c3994decc37"}

    job_id_Ent_TH = {"content_owner_ad_revenue_raw_a1": "10a19692-9ad8-45ef-9ceb-cdd914337818"}

    job_id_Aff_TH = {"content_owner_ad_revenue_raw_a1": "9f2c221b-3ec2-419f-9e4c-d8aa7494b2db"}
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pops-dab1c699446f.json"
    client = bigquery.Client()
    newest_month_BG = get_lastest_month_func('''SELECT DATE(MAX(_PARTITIONTIME)) AS `newest_month_BG`
                       FROM `pops-204909.yt_kids.p_content_owner_ad_revenue_raw_a1_yt_kids`''')
    newest_month_BG = datetime.datetime(int(str(newest_month_BG).split("-")[0]),int(str(newest_month_BG).split("-")[1]),1)
    newest_month_BG_NEXT = add_months(newest_month_BG,1)
    newest_month_BG = str(newest_month_BG).split(" ")[0]
    print(newest_month_BG,newest_month_BG_NEXT)
    check_update = False
    for file in check_files("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\"):
        if str(newest_month_BG) not in str(file):
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
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Music[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Music[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "KIDS":
                    for job_id_num in job_id_Kids:
                        print("--------------------------"+job_id_num+"--------------------------")
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Kids[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Kids[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "ENT":
                    for job_id_num in job_id_Ent:
                        print("--------------------------"+job_id_num+"--------------------------")
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Ent[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Ent[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "AFFILIATE":
                    for job_id_num in job_id_Aff:
                        print("--------------------------"+job_id_num+"--------------------------")
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Aff[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Aff[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "MUSIC-TH":
                    for job_id_num in job_id_Music_TH:
                        print("--------------------------"+job_id_num+"--------------------------")
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Music_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Music_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "ENT-TH":
                    for job_id_num in job_id_Ent_TH:
                        print("--------------------------"+job_id_num+"--------------------------")
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Ent_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Ent_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                elif content_owner_num == "AFF-TH":
                    for job_id_num in job_id_Aff_TH:
                        print("--------------------------"+job_id_num+"--------------------------")
                        try:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Aff_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        except ConnectionAbortedError:
                            report_url = get_report_url_from_user(retrieve_reports(youtube_reporting,newest_month_BG=newest_month_BG,newest_month_BG_NEXT=newest_month_BG_NEXT,jobId=job_id_Aff_TH[str(job_id_num)],onBehalfOfContentOwner=content_owner[str(content_owner_num)]))
                        download_report(youtube_reporting, report_url, content_owner_num+"_"+job_id_num+"_"+str(newest_month_BG)+".txt")
                        print("---------------------"+content_owner_num+" - "+job_id_num+" DONE! ---------------------")
                else:
                    print("Does not match content_owner_num")
        except (HttpError,socket.timeout) as e:
            print('An HTTP error %d occurred:\n %s' % (e,e.content))
# # type: ignore