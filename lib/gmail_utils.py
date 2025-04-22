
import os.path
import base64
from email import message_from_bytes
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pickle

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
TOKEN_PATH = 'token.pickle'
CREDENTIALS_PATH = 'credentials.json'

def authenticate_gmail():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
    service = build('gmail', 'v1', credentials=creds)
    return service

def fetch_emails_with_label(service, label_name="Compass", max_results=10):
    results = service.users().messages().list(userId='me', labelIds=[label_name], maxResults=max_results).execute()
    messages = results.get('messages', [])
    email_bodies = []

    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='raw').execute()
        raw_data = base64.urlsafe_b64decode(msg_data['raw'].encode('ASCII'))
        mime_msg = message_from_bytes(raw_data)
        if mime_msg.is_multipart():
            for part in mime_msg.walk():
                content_type = part.get_content_type()
                if content_type == 'text/html':
                    email_bodies.append(part.get_payload(decode=True).decode('utf-8', errors='ignore'))
        else:
            email_bodies.append(mime_msg.get_payload(decode=True).decode('utf-8', errors='ignore'))

    return email_bodies
