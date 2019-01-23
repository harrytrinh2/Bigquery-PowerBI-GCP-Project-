import argparse
import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from io import FileIO

CLIENT_SECRETS_FILE = "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']
API_SERVICE_NAME = 'youtubereporting'
API_VERSION = 'v1'


# https://google-auth-oauthlib.readthedocs.io/en/latest/reference/google_auth_oauthlib.flow.html

# flow = InstalledAppFlow.from_client_secrets_file(
#     "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json",
#     scopes=['https://www.googleapis.com/auth/yt-analytics-monetary.readonly'])
#
# cred = flow.run_local_server(
#     host='localhost',
#     port=8088,
#     authorization_prompt_message='Please visit this URL: {url}',
#     success_message='The auth flow is complete; you may close this window.',
#     open_browser=True)
#
# with open('refresh.token', 'w+') as f:
#     f.write(cred._refresh_token)
#
# print('Refresh Token:', cred._refresh_token)
# print('Saved Refresh Token to file: refresh.token')

def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server()
    with open('refresh.token', 'w+') as f:
        f.write(credentials._refresh_token)
    print('Refresh Token:', credentials._refresh_token)
    print('Saved Refresh Token to file: refresh.token')
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
get_authenticated_service()