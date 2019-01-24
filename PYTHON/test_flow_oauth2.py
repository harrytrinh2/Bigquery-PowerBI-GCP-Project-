import argparse
import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
import requests
from io import FileIO
from oauth2client import client, GOOGLE_TOKEN_URI

CLIENT_SECRETS_FILE = "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']
API_SERVICE_NAME = 'youtubereporting'
API_VERSION = 'v1'

def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server()
    with open('refresh.token', 'w+') as f:
        f.write(credentials._refresh_token)
    print('Refresh Token:', credentials._refresh_token)
    print('Saved Refresh Token to file: refresh.token')
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


CLIENT_ID = "929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com"
CLIENT_SECRET = "YHDd4FrEFtqjhIkZhprwUMuy"
REFRESH_TOKEN = "1/JqgakzyJwsSnyBijkgXdb4Aq0n1IEhVig09kla5qCqE"

credentials = client.OAuth2Credentials(
    access_token = "ya29.GlubBs2CfFIMOsQRkqSxgAyff5rQ8aiu1IWI6j2Ery5MsuL4VOnr9s7owicF0C_vgM8USc1IDY03jXxWlQn7dCjn2MMa5Gzh6LWZlxqLdLnU2ib8YXPR8nialM1F",
    client_id = CLIENT_ID,
    client_secret = CLIENT_SECRET,
    refresh_token = REFRESH_TOKEN,
    token_expiry = 3600,
    token_uri = "https://oauth2.googleapis.com/token",
    scopes= "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    user_agent="Bearer",
    revoke_uri= None)

http = credentials.authorize(httplib2.Http())
print(http)