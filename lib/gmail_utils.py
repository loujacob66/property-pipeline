import os.path
import base64
from email import message_from_bytes
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pickle

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_script_dir():
    """Get the absolute path to the directory containing this script."""
    return os.path.dirname(os.path.abspath(__file__))

def get_project_root():
    """Get the absolute path to the project root (where config/credentials.json is located)."""
    env_root = os.environ.get("PROPERTY_PIPELINE_ROOT")
    if env_root and os.path.exists(os.path.join(env_root, "config", "credentials.json")):
        return os.path.abspath(env_root)
    # Traverse up from this file's directory
    cur = os.path.dirname(os.path.abspath(__file__))
    while True:
        candidate = os.path.join(cur, "config", "credentials.json")
        if os.path.exists(candidate):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    raise FileNotFoundError("Could not find project root containing config/credentials.json")

def get_credentials_path():
    """Get the path to the credentials file relative to the project root."""
    return os.path.join(get_project_root(), "config", "credentials.json")

def get_token_path():
    """Get the path to the token file relative to the project root."""
    return os.path.join(get_project_root(), "config", "token.pickle")

def authenticate_gmail(credentials_path=None, token_path=None):
    """
    Authenticate with Gmail API.
    
    Args:
        credentials_path (str, optional): Path to credentials.json file
        token_path (str, optional): Path to token.pickle file
    
    Returns:
        service: Authenticated Gmail service
    """
    # Use provided paths or default to config directory relative to script
    credentials_path = credentials_path or get_credentials_path()
    token_path = token_path or get_token_path()
    
    print(f"🔐 Authenticating with Gmail...")
    print(f"Using credentials file: {credentials_path}")
    print(f"Using token file: {token_path}")
    
    try:
        creds = None
        if os.path.exists(token_path):
            print("📝 Loading existing token...")
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("🔄 Refreshing expired token...")
                creds.refresh(Request())
            else:
                print("🔑 Getting new token...")
                if not os.path.exists(credentials_path):
                    raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
            
            print("💾 Saving token...")
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
        
        print("🔨 Building Gmail service...")
        service = build('gmail', 'v1', credentials=creds)
        
        # Test the connection
        print("🔍 Testing connection...")
        profile = service.users().getProfile(userId='me').execute()
        print(f"✅ Connected to Gmail account: {profile['emailAddress']}")
        
        return service
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def fetch_emails_with_label(service, label_id, max_results=10):
    """Fetch emails with a specific label."""
    try:
        print(f"🔍 Searching for emails with label ID: {label_id}")
        
        # Get list of email IDs with this label
        results = service.users().messages().list(
            userId='me',
            q=f'label:{label_id}',  # Use label: prefix in query
            maxResults=max_results
        ).execute()
        
        messages = results.get('messages', [])
        if not messages:
            print("⚠️ No messages found with this label")
            return []
        
        print(f"✅ Found {len(messages)} messages")
        
        # Fetch each email's content
        emails = []
        for message in messages:
            print(f"📧 Fetching message {message['id']}")
            msg = service.users().messages().get(
                userId='me',
                id=message['id'],
                format='full'
            ).execute()
            
            # Print message details for debugging
            if 'payload' in msg and 'headers' in msg['payload']:
                for header in msg['payload']['headers']:
                    if header['name'] in ['Subject', 'Date']:
                        print(f"{header['name']}: {header['value']}")
            
            # Get HTML content from payload
            html_content = None
            
            # First try to get HTML content directly from payload
            if 'payload' in msg and msg['payload'].get('mimeType') == 'text/html':
                data = msg['payload'].get('body', {}).get('data')
                if data:
                    html_content = base64.urlsafe_b64decode(data).decode('utf-8')
                    print("✅ Found HTML content in main payload")
            
            # If not found, look in parts
            if not html_content and 'payload' in msg and 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part['mimeType'] == 'text/html':
                        data = part.get('body', {}).get('data')
                        if data:
                            html_content = base64.urlsafe_b64decode(data).decode('utf-8')
                            print("✅ Found HTML content in message parts")
                            break
            
            if html_content:
                print("✅ Successfully extracted HTML content")
                emails.append({
                    'id': message['id'],
                    'html_content': html_content
                })
            else:
                print("⚠️ No HTML content found in message")
        
        print(f"✅ Successfully processed {len(emails)} emails")
        return emails
    except Exception as e:
        print(f"❌ Error fetching emails: {e}")
        import traceback
        traceback.print_exc()
        return []
