# app.py
import os
import base64
import json
import traceback
import secrets
import requests
from flask import Flask, request, redirect, render_template, jsonify, current_app
from dotenv import load_dotenv
from supabase import create_client
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
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

# ---------- Helpers ----------
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
    return render_template('admin.html', connect_url="/auth/google/connect")

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

@app.route('/auth/google/callback')
def auth_google_callback():
    try:
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
            app.logger.error("Token exchange failed: %s", r.text)
            return f"token exchange error: {r.text}", 500

        tokens = r.json()
        refresh_token = tokens.get('refresh_token')
        access_token = tokens.get('access_token')

        if not refresh_token:
            return "No refresh token returned. Try reconnecting with prompt=consent.", 400

        whoami = requests.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if whoami.status_code != 200:
            return "failed to fetch userinfo", 500

        profile = whoami.json()
        email = profile.get('email')
        display_name = profile.get('name') or email

        enc = aesgcm_encrypt(refresh_token)
        payload = {
            "email": email,
            "display_name": display_name,
            "encrypted_refresh_token": enc,
            "scopes": tokens.get('scope', "").split(" ")
        }

        res = supabase.table("gmail_accounts").upsert(payload, on_conflict=["email"]).execute()
        if getattr(res, "error", None):
            return "DB error storing Gmail account", 500

        return f"Connected {email} — you can close this window."
    except Exception:
        app.logger.error("Unhandled exception:\n%s", traceback.format_exc())
        return "Internal server error", 500

@app.route('/api/subscribe', methods=['POST'])
def api_subscribe():
    try:
        body = request.get_json(force=True)
        email = (body.get('email') or "").strip().lower()
        name = body.get('name', None)

        try:
            validate_email(email)
        except EmailNotValidError as e:
            return jsonify({"error": "invalid_email", "detail": str(e)}), 400

        res = supabase.table("subscribers").upsert(
            {"email": email, "name": name}, on_conflict=["email"]
        ).execute()

        if getattr(res, "error", None):
            return jsonify({"error": "db_error", "detail": str(res.error)}), 500

        data = getattr(res, "data", None)
        subscriber = data[0] if isinstance(data, list) and data else data
        return jsonify({"ok": True, "subscriber": subscriber}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/unsubscribe')
def unsubscribe():
    email = request.args.get('email')
    if not email:
        return "missing email", 400
    res = supabase.table("subscribers").update({"unsubscribed": True}).match({"email": email}).execute()
    if getattr(res, "error", None):
        return "Error unsubscribing", 500
    return "You have been unsubscribed. Thank you."

@app.route('/api/newsletter/queue', methods=['POST'])
def api_queue_newsletter():
    body = request.get_json(force=True)
    title = body.get('title', '')
    subject = body.get('subject')
    html = body.get('body')
    if not subject or not html:
        return jsonify({"error": "subject and body required"}), 400

    # Insert newsletter
    res = supabase.table("newsletters").insert(
        {"title": title, "subject": subject, "body": html, "status": "queued"}
    ).execute()
    if getattr(res, "error", None):
        return jsonify({"error": str(res.error)}), 500

    newsletter = res.data[0]

    # Fetch active subscribers
    subs = supabase.table("subscribers").select("id,email").eq("unsubscribed", False).execute()
    if getattr(subs, "error", None):
        return jsonify({"error": str(subs.error)}), 500
    rows = subs.data

    # Bulk insert into send_queue
    CHUNK = 500
    total = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        objs = [{
            "newsletter_id": newsletter["id"],
            "subscriber_email": s["email"],
            "subscriber_id": s["id"],
        } for s in chunk]
        r = supabase.table("send_queue").insert(objs).execute()
        if getattr(r, "error", None):
            return jsonify({"error": str(r.error)}), 500
        total += len(objs)

    return jsonify({"ok": True, "newsletter_id": newsletter["id"], "queued": total})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
