import google.oauth2.credentials
import google_auth_oauthlib.flow
import requests
CLIENT_SECRETS_FILE = "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']


# // Call refreshToken which creates a new Access Token
# access_token = refreshToken(client_id, client_secret, refresh_token)
#
# // Pass the new Access Token to Credentials() to create new credentials
# credentials = google.oauth2.credentials.Credentials(access_token)
#
# // This function creates a new Access Token using the Refresh Token
# // and also refreshes the ID Token (see comment below).
#
client_id = "929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com"
project_id= "pops-204909"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_secret = "YHDd4FrEFtqjhIkZhprwUMuy"
redirect_uris = ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
refresh_token = "1/RinJvsjGrAUvBj3QoHsHMvopmsf-7U0x1KCvhpo0cq0"

import google.oauth2.credentials
import google_auth_oauthlib.flow

# Use the client_secret.json file to identify the application requesting
# authorization. The client ID (from that file) and access scopes are required.
flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
    CLIENT_SECRETS_FILE,scopes=['https://www.googleapis.com/auth/yt-analytics-monetary.readonly'])

# Indicate where the API server will redirect the user after the user completes
# the authorization flow. The redirect URI is required. The value must exactly
# match one of the authorized redirect URIs for the OAuth 2.0 client, which you
# configured in the API Console. If this value doesn't match an authorized URI,
# you will get a 'redirect_uri_mismatch' error.
flow.redirect_uri = 'https://www.example.com/oauth2callback'

# Generate URL for request to Google's OAuth 2.0 server.
# Use kwargs to set optional request parameters.
authorization_url, state = flow.authorization_url(
    # Enable offline access so that you can refresh an access token without
    # re-prompting the user for permission. Recommended for web server apps.
    access_type='offline',
    # Enable incremental authorization. Recommended as a best practice.
    include_granted_scopes='true')
