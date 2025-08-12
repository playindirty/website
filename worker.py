# worker.py
import os
import requests
import base64
from email.mime.text import MIMEText
from app import supabase, aesgcm_decrypt, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET  # reuse from your app
from datetime import datetime

def get_gmail_access_token(enc_refresh_token):
    refresh_token = aesgcm_decrypt(enc_refresh_token)
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    r = requests.post(token_url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

def build_email_message(sender, to, subject, html_body):
    msg = MIMEText(html_body, "html")
    msg["to"] = to
    msg["from"] = sender
    msg["subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return {"raw": raw}

def send_queued():
    gmail_acc = supabase.table("gmail_accounts").select("*").limit(1).execute()
    if not gmail_acc.data:
        print("No Gmail account connected.")
        return

    gmail_acc = gmail_acc.data[0]
    access_token = get_gmail_access_token(gmail_acc["encrypted_refresh_token"])

    queued = supabase.table("send_queue").select("*").is_("sent_at", None).limit(50).execute()
    if not queued.data:
        print("No queued emails.")
        return

    sent_count = 0
    for q in queued.data:
        nl = supabase.table("newsletters").select("*").eq("id", q["newsletter_id"]).single().execute()
        if not nl.data:
            continue
        nl = nl.data

        message = build_email_message(
            sender=gmail_acc["email"],
            to=q["subscriber_email"],
            subject=nl["subject"],
            html_body=nl["body"]
        )

        send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
        resp = requests.post(send_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }, json=message)

        if resp.status_code == 200:
            supabase.table("send_queue").update({"sent_at": datetime.utcnow().isoformat()}).match({"id": q["id"]}).execute()
            sent_count += 1
        else:
            print(f"Failed to send to {q['subscriber_email']}: {resp.text}")

    print(f"✅ Sent {sent_count} emails.")

if __name__ == "__main__":
    send_queued()
