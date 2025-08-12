# app.py
import os
import base64
import json
from flask import Flask, request, redirect, render_template, jsonify
from dotenv import load_dotenv
from supabase import create_client
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import secrets
import requests
from email_validator import validate_email, EmailNotValidError
from urllib.parse import urlencode

load_dotenv()
app = Flask(__name__, template_folder="templates")

# Supabase server-side client (service role)
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Encryption key (32 bytes hex)
ENCRYPTION_KEY = bytes.fromhex(os.environ['ENCRYPTION_KEY'])

GOOGLE_CLIENT_ID = os.environ['GOOGLE_CLIENT_ID']
GOOGLE_CLIENT_SECRET = os.environ['GOOGLE_CLIENT_SECRET']
OAUTH_REDIRECT_URI = os.environ['OAUTH_REDIRECT_URI']

# Small helpers
def aesgcm_encrypt(plaintext: str) -> str:
    aesgcm = AESGCM(ENCRYPTION_KEY)
    nonce = secrets.token_bytes(12)
    ct = aesgcm.encrypt(nonce, plaintext.encode('utf-8'), None)
    return base64.b64encode(nonce + ct).decode('utf-8')

def aesgcm_decrypt(b64text: str) -> str:
    data = base64.b64decode(b64text)
    nonce = data[:12]
    ct = data[12:]
    aesgcm = AESGCM(ENCRYPTION_KEY)
    pt = aesgcm.decrypt(nonce, ct, None)
    return pt.decode('utf-8')

# ---------- Routes ----------
@app.route('/')
def index():
    return render_template('subscribe.html')

@app.route('/admin')
def admin():
    # NOTE: This page is NOT authenticated in this example. Add auth in production.
    return render_template('admin.html', connect_url="/auth/google/connect")

# Start OAuth connect flow for admins to add Gmail accounts
@app.route('/auth/google/connect')
def auth_google_connect():
    scope = [
        "https://www.googleapis.com/auth/gmail.send",
        "openid",
        "email",
        "profile"
    ]
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(scope),
        "access_type": "offline",
        "prompt": "consent"
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return redirect(url)

# OAuth callback: exchange code -> tokens -> store encrypted refresh_token
@app.route('/auth/google/callback')
def auth_google_callback():
    code = request.args.get('code')
    if not code:
        return "Missing code", 400

    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    r = requests.post(token_url, data=data)
    if r.status_code != 200:
        return f"token exchange error: {r.text}", 500
    tokens = r.json()
    refresh_token = tokens.get('refresh_token')
    if not refresh_token:
        # possible if user already granted consent earlier; instruct admin to re-consent with different account
        # but still try to get userinfo using access_token
        access_token = tokens.get('access_token')
    else:
        access_token = tokens.get('access_token')

    # Get user email from tokeninfo endpoint or people endpoint
    whoami = requests.get("https://www.googleapis.com/oauth2/v2/userinfo", headers={"Authorization": f"Bearer {access_token}"})
    if whoami.status_code != 200:
        return "failed to fetch userinfo", 500
    profile = whoami.json()
    email = profile.get('email')
    display_name = profile.get('name') or email

    # If we don't have a refresh token, warn client (first-time consent is needed)
    if not refresh_token:
        return ("No refresh token returned. Make sure you used prompt=consent and this account hasn't already granted a token.\n"
                "Try connecting again and ensure you approve the consent screens."), 400

    enc = aesgcm_encrypt(refresh_token)
    payload = {
        "email": email,
        "display_name": display_name,
        "encrypted_refresh_token": enc,
        "scopes": tokens.get('scope', "").split(" ")
    }

    # Upsert into supabase
    res = supabase.table("gmail_accounts").upsert(payload, on_conflict=["email"]).execute()
    if res.status_code >= 400:
        return f"db error: {res.text}", 500
    return f"Connected {email}. You can close this window."

# Public subscribe API
@app.route('/api/subscribe', methods=['POST'])
def api_subscribe():
    body = request.get_json(force=True)
    email = (body.get('email') or "").strip().lower()
    name = body.get('name', None)
    try:
        validate_email(email)
    except EmailNotValidError as e:
        return jsonify({"error": "invalid email"}), 400

    res = supabase.table("subscribers").upsert({"email": email, "name": name}, on_conflict=["email"]).execute()
    if res.status_code >= 400:
        return jsonify({"error": res.text}), 500
    return jsonify({"ok": True, "subscriber": res.data[0]})

# Unsubscribe link endpoint
@app.route('/unsubscribe')
def unsubscribe():
    email = request.args.get('email')
    if not email:
        return "missing email", 400
    res = supabase.table("subscribers").update({"unsubscribed": True}).match({"email": email}).execute()
    return "You have been unsubscribed. Thank you.", 200

# Admin route to create & queue newsletter
@app.route('/api/newsletter/queue', methods=['POST'])
def api_queue_newsletter():
    # TODO: Add admin authentication here in production
    body = request.get_json(force=True)
    title = body.get('title', '')
    subject = body.get('subject')
    html = body.get('body')
    if not subject or not html:
        return jsonify({"error": "subject and body required"}), 400

    # insert newsletter
    res = supabase.table("newsletters").insert({"title": title, "subject": subject, "body": html, "status": "queued"}).execute()
    if res.status_code >= 400:
        return jsonify({"error": res.text}), 500
    newsletter = res.data[0]

    # fetch active subscribers
    subs = supabase.table("subscribers").select("id,email").eq("unsubscribed", False).execute()
    if subs.status_code >= 400:
        return jsonify({"error": subs.text}), 500
    rows = subs.data

    # bulk insert into send_queue in chunks
    CHUNK = 500
    total = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        objs = []
        for s in chunk:
            objs.append({
                "newsletter_id": newsletter["id"],
                "subscriber_email": s["email"],
                "subscriber_id": s["id"],
            })
        r = supabase.table("send_queue").insert(objs).execute()
        if r.status_code >= 400:
            return jsonify({"error": r.text}), 500
        total += len(objs)

    return jsonify({"ok": True, "newsletter_id": newsletter["id"], "queued": total})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
