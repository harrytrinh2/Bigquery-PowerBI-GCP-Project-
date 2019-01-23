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
def refreshToken(client_id, client_secret, refresh_token):
        params = {
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token
        }

        authorization_url = "https://www.googleapis.com/oauth2/v4/token"
        r = requests.post(authorization_url, data=params)
        if r.ok:
            print(r.json()['access_token'])
            return r.json()['access_token']
        else:
            return None
