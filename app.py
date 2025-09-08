# app.py
import os
import base64
import json
import traceback
import secrets
import csv
import io
import requests
from datetime import datetime, timedelta
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

def render_email_template(template, lead_data):
    """Replace template variables with lead data and preserve whitespace"""
    rendered = template
    for key, value in lead_data.items():
        if value is None:
            value = ""
        placeholder = "{" + key + "}"
        rendered = rendered.replace(placeholder, str(value))
    
    # Preserve line breaks and spaces by converting them to HTML
    rendered = rendered.replace('\n', '<br>')
    rendered = rendered.replace('  ', '&nbsp;&nbsp;')
    
    return rendered

# ---------- Routes ----------
@app.route('/')
def index():
    return render_template('admin.html')

@app.route('/admin')
def admin():
    return render_template('admin.html')

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



@app.route('/api/campaigns', methods=['GET'])
def api_get_campaigns():
    try:
        campaigns = supabase.table("campaigns").select("*").order("created_at", desc=True).execute()
        return jsonify({"ok": True, "campaigns": campaigns.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/api/campaigns', methods=['POST'])
def api_create_campaign():
    try:
        data = request.get_json(force=True)
        
        # Create campaign
        campaign_data = {
            "name": data.get('name'),
            "subject": data.get('subject'),
            "body": data.get('body'),
            "list_name": data.get('list_name'),
            "send_immediately": data.get('send_immediately', False)
        }
        
        # Handle follow-ups
        follow_ups = data.get('follow_ups', [])
        
        # Insert campaign
        result = supabase.table("campaigns").insert(campaign_data).execute()
        if getattr(result, "error", None):
            return jsonify({"error": "db_error", "detail": str(result.error)}), 500
        
        campaign = result.data[0]
        campaign_id = campaign['id']
        
        # Add follow-ups if any
        if follow_ups:
            for i, follow_up in enumerate(follow_ups):
                follow_up_data = {
                    "campaign_id": campaign_id,
                    "subject": follow_up.get('subject'),
                    "body": follow_up.get('body'),
                    "days_after_previous": follow_up.get('days_after', 1),
                    "sequence": i + 1
                }
                supabase.table("campaign_followups").insert(follow_up_data).execute()
        
        # If sending immediately, queue the first emails
        if data.get('send_immediately'):
            # Get leads for this list
            leads = supabase.table("leads").select("*").eq("list_name", data.get('list_name')).execute()
            
            if leads.data:
                # Queue initial emails
                email_queue = []
                for lead in leads.data:
                    # Render template with lead data
                    rendered_subject = render_email_template(data.get('subject'), lead)
                    rendered_body = render_email_template(data.get('body'), lead)
                    
                    email_queue.append({
                        "campaign_id": campaign_id,
                        "lead_id": lead['id'],
                        "lead_email": lead['email'],
                        "subject": rendered_subject,
                        "body": rendered_body,
                        "sequence": 0,  # 0 for initial email
                        "scheduled_for": datetime.utcnow().isoformat()
                    })
                
                # Insert in chunks
                CHUNK_SIZE = 100
                for i in range(0, len(email_queue), CHUNK_SIZE):
                    chunk = email_queue[i:i+CHUNK_SIZE]
                    supabase.table("email_queue").insert(chunk).execute()
        
        return jsonify({"ok": True, "campaign": campaign}), 200
        
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/api/queue-followup', methods=['POST'])
def api_queue_followup():
    try:
        data = request.get_json(force=True)
        campaign_id = data.get('campaign_id')
        sequence = data.get('sequence')
        
        if not campaign_id or sequence is None:
            return jsonify({"error": "campaign_id and sequence are required"}), 400
        
        # Get campaign and follow-up details
        campaign = supabase.table("campaigns").select("*").eq("id", campaign_id).single().execute()
        follow_up = supabase.table("campaign_followups").select("*").eq("campaign_id", campaign_id).eq("sequence", sequence).single().execute()
        
        if not campaign.data or not follow_up.data:
            return jsonify({"error": "Campaign or follow-up not found"}), 404
        
        # Get leads for this campaign
        leads = supabase.table("leads").select("*").eq("list_name", campaign.data['list_name']).execute()
        
        if not leads.data:
            return jsonify({"ok": True, "queued": 0}), 200
        
        # Calculate send date (days after previous email)
        days_delay = follow_up.data['days_after_previous']
        send_date = datetime.utcnow() + timedelta(days=days_delay)
        
        # Queue follow-up emails
        email_queue = []
        for lead in leads.data:
            # Render template with lead data
            rendered_subject = render_email_template(follow_up.data['subject'], lead)
            rendered_body = render_email_template(follow_up.data['body'], lead)
            
            email_queue.append({
                "campaign_id": campaign_id,
                "lead_id": lead['id'],
                "lead_email": lead['email'],
                "subject": rendered_subject,
                "body": rendered_body,
                "sequence": sequence,
                "scheduled_for": send_date.isoformat()
            })
        
        # Insert in chunks
        CHUNK_SIZE = 100
        total_queued = 0
        for i in range(0, len(email_queue), CHUNK_SIZE):
            chunk = email_queue[i:i+CHUNK_SIZE]
            result = supabase.table("email_queue").insert(chunk).execute()
            total_queued += len(chunk)
        
        return jsonify({"ok": True, "queued": total_queued}), 200
        
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500


# Add these new routes to your app.py

# Add this import at the top of app.py
import traceback

# Update the api_get_lead_lists function
@app.route('/api/leads/lists', methods=['GET'])
def api_get_lead_lists():
    try:
        # Get unique list names with counts using direct query instead of RPC
        query = supabase.table("leads").select("list_name").execute()
        
        # Manual counting since we can't use RPC
        list_counts = {}
        for lead in query.data:
            list_name = lead.get('list_name', 'Unknown')
            if list_name:
                list_counts[list_name] = list_counts.get(list_name, 0) + 1
        
        lists = [{"list_name": name, "lead_count": count} for name, count in list_counts.items()]
        return jsonify({"ok": True, "lists": lists}), 200
    except Exception as e:
        app.logger.error("Error in api_get_lead_lists: %s", traceback.format_exc())
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

# Update the api_import_leads function with better error handling
@app.route('/api/leads/import', methods=['POST'])
def api_import_leads():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        
        file = request.files['file']
        list_name = request.form.get('list_name', 'Imported List')
        
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({"error": "Only CSV files are supported"}), 400
        
        # Read and parse CSV
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_input = csv.DictReader(stream)
        
        # Check required columns
        if 'email' not in csv_input.fieldnames:
            return jsonify({"error": "CSV must contain an 'email' column"}), 400
        
        # Process rows and remove duplicates
        leads_dict = {}  # Use dictionary to track unique emails
        for row in csv_input:
            if not row.get('email'):
                continue
                
            # Clean the row data
            cleaned_row = {}
            for key, value in row.items():
                if value is not None:
                    cleaned_row[key.strip().lower()] = value.strip()
            
            email = cleaned_row.get('email', '').lower()
            
            # Skip if email is invalid
            try:
                validate_email(email)
            except EmailNotValidError:
                continue
            
            lead_data = {
                "email": email,
                "name": cleaned_row.get('name', ''),
                "last_name": cleaned_row.get('last_name', cleaned_row.get('last name', '')),
                "city": cleaned_row.get('city', ''),
                "brokerage": cleaned_row.get('brokerage', ''),
                "service": cleaned_row.get('service', ''),
                "list_name": list_name,
                "custom_fields": {k: v for k, v in cleaned_row.items() if k not in ['email', 'name', 'last_name', 'last name', 'city', 'brokerage', 'service', 'list_name']}
            }
            
            # Keep only the last occurrence of each email
            leads_dict[email] = lead_data
        
        leads = list(leads_dict.values())
        
        # Store in database
        if leads:
            # Insert in chunks to avoid payload size issues
            CHUNK_SIZE = 100
            imported_count = 0
            for i in range(0, len(leads), CHUNK_SIZE):
                chunk = leads[i:i+CHUNK_SIZE]
                result = supabase.table("leads").upsert(chunk, on_conflict="email").execute()
                if getattr(result, "error", None):
                    return jsonify({"error": "db_error", "detail": str(result.error)}), 500
                imported_count += len(chunk)
        
        return jsonify({
            "ok": True, 
            "imported": len(leads),
            "sample": leads[0] if leads else {}
        }), 200
        
    except Exception as e:
        app.logger.error("Error in api_import_leads: %s", traceback.format_exc())
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/api/leads/<list_name>', methods=['GET'])
def api_get_leads_by_list(list_name):
    try:
        leads = supabase.table("leads").select("*").eq("list_name", list_name).execute()
        return jsonify({"ok": True, "leads": leads.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

# Add to app.py
@app.route('/api/account-status', methods=['GET'])
def api_get_account_status():
    try:
        today = date.today().isoformat()
        
        # Get all accounts with their daily counts
        accounts = supabase.table("gmail_accounts").select("*").execute()
        
        statuses = []
        for account in accounts.data:
            # Get today's count for this account
            count_data = supabase.table("daily_email_counts") \
                .select("count") \
                .eq("gmail_account", account["email"]) \
                .eq("date", today) \
                .execute()
            
            if count_data.data:
                count = count_data.data[0]["count"]
            else:
                count = 0
                
            statuses.append({
                "email": account["email"],
                "display_name": account["display_name"],
                "sent_today": count,
                "remaining_today": 50 - count
            })
        
        return jsonify({"ok": True, "accounts": statuses}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
