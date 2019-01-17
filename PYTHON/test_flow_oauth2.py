from google_auth_oauthlib.flow import InstalledAppFlow

flow = InstalledAppFlow.from_client_secrets_file(
    "client_secret_929791903032-hpdm8djidqd8o5nqg2gk66efau34ea6q.apps.googleusercontent.com.json",
    scopes=['profile', 'email'])

flow.run_local_server(open_browser=False)

session = flow.authorized_session()

profile_info = session.get(
    'https://www.googleapis.com/userinfo/v2/me').json()

print(profile_info)
