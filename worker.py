# worker.py
import os
import time
import base64
import json
import requests
from dotenv import load_dotenv
from supabase import create_client
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from email.message import EmailMessage
from urllib.parse import urlencode

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ENCRYPTION_KEY = bytes.fromhex(os.environ['ENCRYPTION_KEY'])

GOOGLE_CLIENT_ID = os.environ['GOOGLE_CLIENT_ID']
GOOGLE_CLIENT_SECRET = os.environ['GOOGLE_CLIENT_SECRET']

def decrypt_token(b64):
    data = base64.b64decode(b64)
    nonce = data[:12]; ct = data[12:]
    aesgcm = AESGCM(ENCRYPTION_KEY)
    pt = aesgcm.decrypt(nonce, ct, None)
    return pt.decode('utf-8')

def get_access_token_from_refresh(refresh_token):
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    r = requests.post(token_url, data=data)
    if r.status_code != 200:
        raise Exception(f"token refresh error: {r.status_code} {r.text}")
    return r.json()["access_token"]

def make_raw_message(from_addr, to_addr, subject, html_body, reply_to=None):
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content("This is an HTML email. If you see this text, your client doesn't render HTML.")
    msg.add_alternative(html_body, subtype='html')
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8').rstrip("=")
    return raw

def send_via_gmail_api(access_token, raw):
    url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json={"raw": raw})
    return resp

def process_batch(limit=20):
    # fetch pending send_queue items due for send
    q = supabase.table("send_queue").select("*").eq("status","pending").lte("next_try", "now()").limit(limit).execute()
    if q.status_code != 200:
        print("Failed to fetch queue", q.text)
        return
    rows = q.data
    if not rows:
        return

    # fetch gmail accounts
    acc = supabase.table("gmail_accounts").select("*").execute()
    if acc.status_code != 200 or not acc.data:
        print("No gmail accounts configured")
        return
    accounts = acc.data

    for row in rows:
        try:
            # round-robin or random pick
            account = accounts[hash(row['id']) % len(accounts)]
            refresh_enc = account['encrypted_refresh_token']
            refresh_token = decrypt_token(refresh_enc)
            access_token = get_access_token_from_refresh(refresh_token)

            # get newsletter body
            nl = supabase.table("newsletters").select("*").eq("id", row['newsletter_id']).single().execute()
            if nl.status_code != 200:
                raise Exception("missing newsletter")
            nl = nl.data

            unsubscribe_url = f"{os.environ.get('APP_BASE_URL','http://localhost:5000')}/unsubscribe?email={row['subscriber_email']}"
            html_with_unsub = nl['body'] + f"<hr/><p style='font-size:12px'><a href='{unsubscribe_url}'>Unsubscribe</a></p>"

            from_addr = f"{account.get('display_name')} <{account.get('email')}>"
            raw = make_raw_message(from_addr, row['subscriber_email'], nl['subject'], html_with_unsub)
            resp = send_via_gmail_api(access_token, raw)

            if 200 <= resp.status_code < 300:
                # success
                supabase.table("send_queue").update({"status":"sent","gmail_account_id": account['id'], "sent_at": "now()"}).eq("id", row['id']).execute()
                supabase.table("send_logs").insert({
                    "send_queue_id": row['id'],
                    "newsletter_id": nl['id'],
                    "gmail_account_id": account['id'],
                    "subscriber_email": row['subscriber_email'],
                    "status": "sent",
                    "raw_response": resp.json()
                }).execute()
            else:
                # retry logic: increment attempts and exponential backoff
                attempts = (row.get('attempts') or 0) + 1
                backoff_secs = min(60 * 60 * attempts, 24 * 60 * 60)
                next_try = f"now() + interval '{backoff_secs} seconds'"
                supabase.table("send_queue").update({
                    "attempts": attempts,
                    "last_error": resp.text,
                    "next_try": next_try,
                    "status": "pending"
                }).eq("id", row['id']).execute()

                supabase.table("send_logs").insert({
                    "send_queue_id": row['id'],
                    "newsletter_id": nl['id'],
                    "gmail_account_id": account['id'],
                    "subscriber_email": row['subscriber_email'],
                    "status": "failed",
                    "raw_response": {"status": resp.status_code, "text": resp.text}
                }).execute()

        except Exception as e:
            print("error sending", e)
            attempts = (row.get('attempts') or 0) + 1
            backoff_secs = min(60 * 60 * attempts, 24 * 60 * 60)
            next_try = f"now() + interval '{backoff_secs} seconds'"
            supabase.table("send_queue").update({
                "attempts": attempts,
                "last_error": str(e),
                "next_try": next_try
            }).eq("id", row['id']).execute()

if __name__ == "__main__":
    print("Worker started")
    while True:
        try:
            process_batch(limit=30)
        except Exception as e:
            print("worker exception", e)
        time.sleep(5)
