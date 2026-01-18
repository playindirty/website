# app.py
import os
import base64
import json
import traceback
import secrets
import csv
import io
import requests
import smtplib
import imaplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone, date
from flask import Flask, request, redirect, render_template, jsonify, current_app
from dotenv import load_dotenv
from supabase import create_client
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from email_validator import validate_email, EmailNotValidError
from urllib.parse import urlencode
import urllib.parse


# Supabase server-side client (service role)
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Encryption key (32 bytes hex)
ENCRYPTION_KEY = bytes.fromhex(os.environ['ENCRYPTION_KEY'])

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
# Add this import at the top of app.py
from flask_cors import CORS

load_dotenv()
app = Flask(__name__, template_folder="templates")
#"https://closefaster.vercel.app",
#            "http://localhost:3000",  # For local development
 #           "http://127.0.0.1:3000",  # For local development
  #          "https://playindirty.github.io/website/",
   #         "https://playindirty.github.io/",
    #        "https://closefaster.vercel.app/templates/demooff",
     #       "https://closefaster.vercel.app/templates/",
      #      "https://closefaster.vercel.app/",
# Add CORS support - allow requests from your Vercel domain
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "replyzeai.com",
            "replyzeai.com/demooff",
        ],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "Origin"]
    }
})

# Your existing routes...
# ---------- Routes ----------
@app.route('/')
def index():
    return render_template('admin.html')

@app.route('/admin')
def admin():
    return render_template('admin.html')

# Remove Google OAuth routes and add SMTP account routes
@app.route('/api/smtp-accounts', methods=['GET'])
def api_get_smtp_accounts():
    try:
        accounts = supabase.table("smtp_accounts").select("*").execute()
        return jsonify({"ok": True, "accounts": accounts.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

# Update the account status endpoint to use SMTP accounts
@app.route('/api/account-status', methods=['GET'])
def api_get_account_status():
    try:
        today = date.today().isoformat()
        
        # Get all SMTP accounts with their daily counts
        accounts = supabase.table("smtp_accounts").select("*").execute()
        
        statuses = []
        for account in accounts.data:
            # Get today's count for this account
            count_data = supabase.table("daily_email_counts") \
                .select("count") \
                .eq("email_account", account["email"]) \
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
                "remaining_today": 100 - count
            })
        
        return jsonify({"ok": True, "accounts": statuses}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

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
                        "scheduled_for": datetime.now(timezone.utc).isoformat()
                    })
                
                # Insert in chunks
                CHUNK_SIZE = 100
                for i in range(0, len(email_queue), CHUNK_SIZE):
                    chunk = email_queue[i:i+CHUNK_SIZE]
                    supabase.table("email_queue").insert(chunk).execute()
                
                print(f"DEBUG: Queued {len(email_queue)} emails with scheduled_for: {datetime.now(timezone.utc).isoformat()}")
        
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
        send_date = datetime.now(timezone.utc) + timedelta(days=days_delay)
        
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
        
        # Read file with error handling for encoding issues
        file_content = file.read()
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']
        decoded_content = None
        
        for encoding in encodings:
            try:
                decoded_content = file_content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if decoded_content is None:
            # If all encodings fail, use latin-1 which never fails but may replace some characters
            decoded_content = file_content.decode('latin-1')
        
        stream = io.StringIO(decoded_content, newline=None)
        csv_input = csv.DictReader(stream)
        
        # Check required columns
        if 'email' not in csv_input.fieldnames:
            return jsonify({"error": "CSV must contain an 'email' column"}), 400
        
        # Process rows and remove duplicates
        leads_dict = {}  # Use dictionary to track unique emails
        for row in csv_input:
            if not row.get('email'):
                continue
                
            # Clean the row data and handle encoding issues
            cleaned_row = {}
            for key, value in row.items():
                if value is not None:
                    # Clean the key and value
                    clean_key = key.strip().lower()
                    clean_value = value.strip()
                    
                    # Replace common problematic characters
                    if clean_value:
                        clean_value = clean_value.replace('â€™', "'")  # Common smart quote issue
                        clean_value = clean_value.replace('â€"', '-')  # Common em dash issue
                        clean_value = clean_value.replace('â€"', '"')  # Common quote issue
                    
                    cleaned_row[clean_key] = clean_value
            
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
                # These fields are now explicitly extracted just like city/brokerage
                "custom_fields": {
                    "last_sale": cleaned_row.get('last sale', ''),
                    "open_house": cleaned_row.get('open house', ''),
                    "street": cleaned_row.get('street', ''),
                    "ai_hooks": cleaned_row.get('ai hooks', ''),
                    # Captures any other unexpected columns
                    **{k: v for k, v in cleaned_row.items() if k not in ['email', 'name', 'last_name', 'last name', 'city', 'brokerage', 'service', 'list_name']}
                }
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

@app.route('/api/smtp-accounts', methods=['POST'])
def api_add_smtp_account():
    try:
        data = request.get_json(force=True)
        
        # Test SMTP connection first
        try:
            smtp = smtplib.SMTP(data['smtp_host'], data['smtp_port'])
            smtp.starttls()  # Use TLS for security
            smtp.login(data['smtp_username'], data['smtp_password'])
            smtp.quit()
        except Exception as e:
            return jsonify({"error": "smtp_connection_failed", "detail": str(e)}), 400
        
        # Encrypt password before storing
        encrypted_password = aesgcm_encrypt(data['smtp_password'])
        
        # Store account details
        account_data = {
            "email": data['email'],
            "display_name": data.get('display_name', data['email']),
            "smtp_host": data['smtp_host'],
            "smtp_port": data['smtp_port'],
            "smtp_username": data['smtp_username'],
            "encrypted_smtp_password": encrypted_password,
            "imap_host": data.get('imap_host'),
            "imap_port": data.get('imap_port')
        }
        
        result = supabase.table("smtp_accounts").insert(account_data).execute()
        if getattr(result, "error", None):
            return jsonify({"error": "db_error", "detail": str(result.error)}), 500
        
        return jsonify({"ok": True, "account": result.data[0]}), 200
        
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

# Add this to app.py
@app.route('/api/lead-campaign-accounts', methods=['GET'])
def api_get_lead_campaign_accounts():
    try:
        accounts = supabase.table("lead_campaign_accounts").select("*").execute()
        return jsonify({"ok": True, "accounts": accounts.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500
        
@app.route('/api/responded-leads', methods=['GET'])
def api_get_responded_leads():
    try:
        responded_leads = supabase.table("responded_leads").select("*").order("responded_at", desc=True).execute()
        return jsonify({"ok": True, "responded_leads": responded_leads.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

# Update the route to use integer IDs
@app.route('/track/<lead_id>/<campaign_id>')
def track_click(lead_id, campaign_id):
    try:
        # Get the original URL from query parameters
        url = request.args.get('url')
        if not url:
            return "URL parameter missing", 400
            
        # Decode the URL
        original_url = urllib.parse.unquote(url)
        
        # Get the email_queue_id if available
        email_queue_id = request.args.get('eqid', None)
        
        # Convert IDs to integers if possible
        try:
            lead_id_int = int(lead_id)
        except (ValueError, TypeError):
            lead_id_int = None
            
        try:
            campaign_id_int = int(campaign_id)
        except (ValueError, TypeError):
            campaign_id_int = None
        
        # Record the click in the database
        supabase.table("link_clicks").insert({
            "lead_id": lead_id_int,
            "campaign_id": campaign_id_int,
            "url": original_url,
            "email_queue_id": email_queue_id
        }).execute()
        
        # Redirect to the demo page with lead_id as parameter
        demo_url = "https://replyzeai.com/goods/templates/demooff"
        redirect_url = f"{demo_url}?lead_id={lead_id}&campaign_id={campaign_id}"
        if email_queue_id:
            redirect_url += f"&eqid={email_queue_id}"
            
        return redirect(redirect_url)
        
    except Exception as e:
        print(f"Error tracking click: {str(e)}")
        return "Error tracking click", 500

@app.route('/api/campaigns/<int:campaign_id>/clicks')
def api_get_campaign_clicks(campaign_id):
    try:
        clicks = supabase.table("link_clicks") \
            .select("*, leads(email, name)") \
            .eq("campaign_id", campaign_id) \
            .order("clicked_at", desc=True) \
            .execute()
        
        return jsonify({"ok": True, "clicks": clicks.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/api/leads/<int:lead_id>/clicks')
def api_get_lead_clicks(lead_id):
    try:
        clicks = supabase.table("link_clicks") \
            .select("*, campaigns(name)") \
            .eq("lead_id", lead_id) \
            .order("clicked_at", desc=True) \
            .execute()
        
        return jsonify({"ok": True, "clicks": clicks.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/api/track', methods=['GET'])
def api_track_click():
    try:
        lead_id = request.args.get('lead_id')
        campaign_id = request.args.get('campaign_id')
        url = request.args.get('url')
        email_queue_id = request.args.get('eqid', None)
        
        if not all([lead_id, campaign_id, url]):
            return "Missing parameters", 400
            
        # Record the click in the database
        supabase.table("link_clicks").insert({
            "lead_id": lead_id,
            "campaign_id": campaign_id,
            "url": url,
            "email_queue_id": email_queue_id
        }).execute()
        
        # Redirect to the original URL
        return redirect(url)
        
    except Exception as e:
        print(f"Error tracking click: {str(e)}")
        return "Error tracking click", 500





        
# Add to imports at the top of app.py
# Add these imports at the top of app.py
import json
from datetime import datetime, timezone
from urllib.parse import unquote

# Add these routes to app.py

@app.route('/demo')
def demo():
    return render_template('demo.html', 
                         supabase_url=os.environ['SUPABASE_URL'],
                         supabase_anon_key=os.environ['SUPABASE_ANON_KEY'])


@app.route('/api/generate-reply-prompt', methods=['OPTIONS', 'POST'])
def generate_reply_prompt():
    if request.method == "OPTIONS":
        # Handle preflight request
        response = jsonify({"status": "ok"})
        response.headers.add("Access-Control-Allow-Origin", "https://replyzeai.com")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type")
        return response

    data = request.get_json(force=True)
    prompt = data.get("prompt", "").strip()
    if not prompt:
        return jsonify({"error": "Missing prompt"}), 400

    # Enhanced prompt to generate reply and three follow-ups
    enhanced_prompt = f"""
    Generate a professional real estate agent reply to the following email, and then generate three follow-up emails that would be sent later.
    Format your response exactly as follows:

    === REPLY ===
    [Your main reply here]

    === FOLLOW UP 1 ===
    [First follow-up email]

    === FOLLOW UP 2 ===
    [Second follow-up email]

    === FOLLOW UP 3 ===
    [Third follow-up email]

    Email to respond to:
    {prompt}
    """

    try:
        # Use Groq API instead of GitHub AI models
        GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
        if not GROQ_API_KEY:
            return jsonify({"error": "Groq API key not configured"}), 500
        
        # Make request to Groq API
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.1-8b-instant",  # You can change this to other Groq models
                "messages": [
                    {"role": "system", "content": "You are a professional real estate agent. Generate concise, professional responses that help convert leads into appointments."},
                    {"role": "user", "content": enhanced_prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 1024,
                "top_p": 0.8
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return jsonify({"error": f"Groq API error: {response.status_code}"}), 500
        
        result = response.json()
        full_response = result["choices"][0]["message"]["content"].strip()
        
        # Parse the response to extract reply and follow-ups
        sections = {}
        current_section = None
        lines = full_response.split('\n')
        
        for line in lines:
            line = line.strip()
            if line == "=== REPLY ===":
                current_section = 'reply'
                sections[current_section] = []
            elif line == "=== FOLLOW UP 1 ===":
                current_section = 'follow_up_1'
                sections[current_section] = []
            elif line == "=== FOLLOW UP 2 ===":
                current_section = 'follow_up_2'
                sections[current_section] = []
            elif line == "=== FOLLOW UP 3 ===":
                current_section = 'follow_up_3'
                sections[current_section] = []
            elif current_section and line:
                sections[current_section].append(line)
        
        # Join the lines for each section
        reply = ' '.join(sections.get('reply', [])).strip()
        follow_ups = [
            ' '.join(sections.get('follow_up_1', [])).strip(),
            ' '.join(sections.get('follow_up_2', [])).strip(),
            ' '.join(sections.get('follow_up_3', [])).strip()
        ]
        
        # Remove any empty follow-ups
        follow_ups = [fu for fu in follow_ups if fu]
        
        # Add CORS headers to the response
        response = jsonify({
            "reply": reply,
            "follow_ups": follow_ups
        })
        response.headers.add("Access-Control-Allow-Origin", "https://replyzeai.com")
        return response
        
    except Exception as e:
        print(f"Error generating reply with Groq: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/record-ai-usage', methods=['POST'])
def api_record_ai_usage():
    try:
        data = request.get_json(force=True)
        lead_id = data.get('lead_id')
        
        if not lead_id:
            return jsonify({"error": "Lead ID is required"}), 400
        
        # Convert to integer if needed
        try:
            lead_id = int(lead_id)
        except (ValueError, TypeError):
            pass
        
        # Get the lead's email
        lead = supabase.table("leads") \
            .select("email") \
            .eq("id", lead_id) \
            .single() \
            .execute()
        
        if not lead.data:
            return jsonify({"error": "Lead not found"}), 404
        
        email = lead.data['email']
        
        # Check if we already have a record for this email
        existing = supabase.table("ai_demo_usage") \
            .select("*") \
            .eq("email", email) \
            .execute()
        
        if existing.data:
            # Update existing record
            supabase.table("ai_demo_usage") \
                .update({
                    "usage_count": existing.data[0]['usage_count'] + 1,
                    "last_used_at": datetime.now(timezone.utc).isoformat()
                }) \
                .eq("email", email) \
                .execute()
        else:
            # Create new record
            supabase.table("ai_demo_usage") \
                .insert({
                    "lead_id": lead_id,
                    "email": email,
                    "usage_count": 1,
                    "first_used_at": datetime.now(timezone.utc).isoformat(),
                    "last_used_at": datetime.now(timezone.utc).isoformat()
                }) \
                .execute()
        
        return jsonify({"ok": True}), 200
        
    except Exception as e:
        print(f"Error recording AI usage: {str(e)}")
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500



@app.route('/api/leads/<int:lead_id>', methods=['GET'])
def api_get_lead(lead_id):
    try:
        lead = supabase.table("leads").select("*").eq("id", lead_id).single().execute()
        return jsonify({"ok": True, "lead": lead.data}), 200
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.route('/api/leads/<int:lead_id>/ai-usage', methods=['GET'])
def api_get_lead_ai_usage(lead_id):
    try:
        # Get lead email first
        lead = supabase.table("leads") \
            .select("email") \
            .eq("id", lead_id) \
            .single() \
            .execute()
        
        if not lead.data:
            return jsonify({"ok": True, "ai_usage": None}), 200
        
        # Check AI usage
        ai_usage = supabase.table("ai_demo_usage") \
            .select("*") \
            .eq("email", lead.data['email']) \
            .execute()
        
        return jsonify({
            "ok": True, 
            "ai_usage": ai_usage.data[0] if ai_usage.data else None
        }), 200
        
    except Exception as e:
        return jsonify({"error": "internal_server_error", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
