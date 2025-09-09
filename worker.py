# worker.py
import os
import smtplib
import base64
from email.mime.text import MIMEText
from app import supabase, aesgcm_decrypt
from datetime import datetime, timedelta, date# worker.py
import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, date
from supabase import create_client
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# Initialize Supabase
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Encryption functions
ENCRYPTION_KEY = bytes.fromhex(os.environ['ENCRYPTION_KEY'])

def aesgcm_decrypt(b64text: str) -> str:
    data = base64.b64decode(b64text)
    nonce = data[:12]
    ct = data[12:]
    aesgcm = AESGCM(ENCRYPTION_KEY)
    pt = aesgcm.decrypt(nonce, ct, None)
    return pt.decode('utf-8')

def send_email_via_smtp(account, to_email, subject, html_body):
    """Send email using SMTP"""
    try:
        # Decrypt SMTP password
        smtp_password = aesgcm_decrypt(account["encrypted_smtp_password"])
        
        # Create message
        msg = MIMEText(html_body, "html")
        msg["Subject"] = subject
        msg["From"] = f"{account['display_name']} <{account['email']}>"
        msg["To"] = to_email
        
        # Send email
        smtp = smtplib.SMTP(account["smtp_host"], account["smtp_port"])
        smtp.starttls()  # Use TLS
        smtp.login(account["smtp_username"], smtp_password)
        smtp.send_message(msg)
        smtp.quit()
        return True
    except Exception as e:
        print(f"Error sending email via SMTP: {str(e)}")
        return False

def get_account_for_lead_campaign(lead_id, campaign_id):
    """Get the assigned SMTP account for a lead/campaign combination"""
    try:
        # Check if we already have an account assigned for this lead/campaign
        assignment = supabase.table("lead_campaign_accounts") \
            .select("smtp_account") \
            .eq("lead_id", lead_id) \
            .eq("campaign_id", campaign_id) \
            .execute()
        
        if assignment.data:
            # Get the account details
            account = supabase.table("smtp_accounts") \
                .select("*") \
                .eq("email", assignment.data[0]["smtp_account"]) \
                .single() \
                .execute()
            return account.data
        return None
    except:
        return None

def assign_account_to_lead_campaign(lead_id, campaign_id, account_email):
    """Assign an SMTP account to a lead/campaign combination"""
    supabase.table("lead_campaign_accounts").upsert({
        "lead_id": lead_id,
        "campaign_id": campaign_id,
        "smtp_account": account_email
    }).execute()

def get_all_accounts_with_capacity():
    """Get all SMTP accounts with their current usage and capacity"""
    today = date.today().isoformat()
    
    # Get all accounts
    accounts = supabase.table("smtp_accounts").select("*").execute()
    
    accounts_with_capacity = []
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
            
        # Calculate remaining capacity
        remaining = 50 - count
        
        if remaining > 0:
            accounts_with_capacity.append({
                "account": account,
                "sent_today": count,
                "remaining": remaining
            })
    
    # Sort by remaining capacity (descending) to prioritize accounts with most capacity
    accounts_with_capacity.sort(key=lambda x: x["remaining"], reverse=True)
    return accounts_with_capacity

def update_daily_count(email_account, count):
    """Update the daily count for an account"""
    today = date.today().isoformat()
    
    # Check if record exists
    existing = supabase.table("daily_email_counts") \
        .select("id") \
        .eq("email_account", email_account) \
        .eq("date", today) \
        .execute()
    
    if existing.data:
        # Update existing record
        supabase.table("daily_email_counts") \
            .update({"count": count}) \
            .eq("email_account", email_account) \
            .eq("date", today) \
            .execute()
    else:
        # Create new record
        supabase.table("daily_email_counts") \
            .insert({
                "email_account": email_account,
                "date": today,
                "count": count
            }) \
            .execute()

def send_queued():
    print("DEBUG: send_queued function called")
    # Get queued emails that are scheduled for now or earlier
    queued = (
        supabase.table("email_queue")
        .select("*")
        .is_("sent_at", "null")
        .lte("scheduled_for", datetime.utcnow().isoformat())
        .limit(200)
        .execute()
    )

    if not queued.data:
        print("No queued emails ready to send.")
        return

    # Get all accounts with capacity
    available_accounts = get_all_accounts_with_capacity()
    
    if not available_accounts:
        print("All accounts have reached their daily limit (50 emails).")
        return
        
    print(f"Found {len(available_accounts)} accounts with capacity")
    
    sent_count = 0
    failed_count = 0
    
    # Distribute emails across available accounts
    account_index = 0
    total_accounts = len(available_accounts)
    
    for q in queued.data:
        # Check if there's an assigned account for this lead/campaign
        assigned_account = get_account_for_lead_campaign(q["lead_id"], q["campaign_id"])
        
        if assigned_account:
            # Use the assigned account if it has capacity
            account_found = None
            for acc in available_accounts:
                if acc["account"]["email"] == assigned_account["email"] and acc["remaining"] > 0:
                    account_found = acc
                    break
            
            if account_found:
                account_data = account_found
                account = account_data["account"]
                current_count = account_data["sent_today"]
            else:
                # Skip this email if the assigned account doesn't have capacity
                print(f"Skipping email for {q['lead_email']} - assigned account has no capacity")
                continue
        else:
            # Use round-robin for emails without an assigned account
            if account_index >= total_accounts:
                account_index = 0
                
            account_data = available_accounts[account_index]
            account = account_data["account"]
            current_count = account_data["sent_today"]
            
            # Assign this account to the lead/campaign for future emails
            assign_account_to_lead_campaign(q["lead_id"], q["campaign_id"], account["email"])
        
        try:
            tracked_body = replace_urls_with_tracking(
                 q["body"], 
                 q["lead_id"], 
                 q["campaign_id"],
                 q["id"]  # email_queue_id
            )

            success = send_email_via_smtp(
                account=account,
                to_email=q["lead_email"],
                subject=q["subject"],
                html_body=tracked_body
            )

            if success:
                # Mark as sent
                update_data = {
                    "sent_at": datetime.utcnow().isoformat(),
                    "sent_from": account["email"]
                }
                supabase.table("email_queue").update(update_data).match({"id": q["id"]}).execute()
                
                # Update daily count for this account
                new_count = current_count + 1
                update_daily_count(account["email"], new_count)
                
                # Update our local count
                account_data["sent_today"] = new_count
                account_data["remaining"] = 50 - new_count
                
                # If this account is now at capacity, remove it from available accounts
                if new_count >= 50:
                    available_accounts.pop(account_index)
                    total_accounts = len(available_accounts)
                    if total_accounts == 0:
                        print("All accounts have reached their daily limit.")
                        break
                    # Adjust index if we removed the current account
                    if account_index >= total_accounts:
                        account_index = 0
                else:
                    account_index += 1
                
                # If this is an initial email (sequence 0), schedule the first follow-up
                if q["sequence"] == 0:
                    schedule_followup(q, 1, account["email"])
                
                sent_count += 1
            else:
                print(f"Failed to send to {q['lead_email']}")
                failed_count += 1
                account_index += 1  # Move to next account even on failure
                
        except Exception as e:
            print(f"Error sending email to {q['lead_email']}: {str(e)}")
            failed_count += 1
            account_index += 1  # Move to next account on error

    print(f"✅ Sent {sent_count} emails. Failed: {failed_count}")

def schedule_followup(q, sequence, account_email):
    """Schedule a follow-up email using the same account"""
    # Get the follow-up for this campaign and sequence
    follow_up = (
        supabase.table("campaign_followups")
        .select("*")
        .eq("campaign_id", q["campaign_id"])
        .eq("sequence", sequence)
        .execute()
    )
    
    if follow_up.data:
        follow_up = follow_up.data[0]
        # Get lead data
        lead = supabase.table("leads").select("*").eq("id", q["lead_id"]).single().execute()
        
        if lead.data:
            # Calculate send date
            days_delay = follow_up["days_after_previous"]
            send_date = datetime.utcnow() + timedelta(days=days_delay)
            
            # Render template with lead data
            rendered_subject = render_email_template(follow_up["subject"], lead.data)
            rendered_body = render_email_template(follow_up["body"], lead.data)
            
            # Queue follow-up with the same account
            supabase.table("email_queue").insert({
                "campaign_id": q["campaign_id"],
                "lead_id": q["lead_id"],
                "lead_email": q["lead_email"],
                "subject": rendered_subject,
                "body": rendered_body,
                "sequence": sequence,
                "scheduled_for": send_date.isoformat()
            }).execute()

def render_email_template(template, lead_data):
    """Replace template variables with lead data"""
    rendered = template
    for key, value in lead_data.items():
        if value is None:
            value = ""
        placeholder = "{" + key + "}"
        rendered = rendered.replace(placeholder, str(value))
    return rendered


# Add this import at the top
import urllib.parse
import re

def replace_urls_with_tracking(html_content, lead_id, campaign_id, email_queue_id=None):
    """
    Replace all URLs in HTML content with tracking URLs, including internal links.
    """
    print(f"DEBUG: Processing HTML content for lead {lead_id}, campaign {campaign_id}")
    
    # Get the base URL from environment variable
    app_base_url = os.environ.get('APP_BASE_URL')
    if not app_base_url:
        print("ERROR: APP_BASE_URL environment variable is not set!")
        return html_content
    
    print(f"DEBUG: Using APP_BASE_URL: {app_base_url}")
    
    # Pattern to find href attributes
    pattern = r'href="(.*?)"'
    
    def replace_with_tracking(match):
        original_url = match.group(1)
        print(f"DEBUG: Found URL: {original_url}")
        
        # Skip if it's already a tracking link or mailto link
        if '/track/' in original_url or original_url.startswith('mailto:'):
            print(f"DEBUG: Skipping (already tracking or mailto): {original_url}")
            return match.group(0)
            
        # Encode the original URL
        encoded_url = urllib.parse.quote(original_url)
        
        # Build tracking URL
        tracking_url = f"{app_base_url}/track/{lead_id}/{campaign_id}?url={encoded_url}"
        
        # Add email_queue_id if available
        if email_queue_id:
            tracking_url += f"&eqid={email_queue_id}"
            
        print(f"DEBUG: Replacing with: {tracking_url}")
        return f'href="{tracking_url}"'
    
    # Replace all URLs
    result = re.sub(pattern, replace_with_tracking, html_content)
    print(f"DEBUG: Final result: {result}")
    return result



if __name__ == "__main__":
    send_queued()
