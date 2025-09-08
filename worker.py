# worker.py
import os
import requests
import base64
from email.mime.text import MIMEText
from app import supabase, aesgcm_decrypt, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
from datetime import datetime, timedelta, date

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

def get_account_with_capacity():
    """Get a Gmail account that hasn't reached its daily limit"""
    today = date.today().isoformat()
    
    # Get all accounts with their daily counts
    accounts = supabase.table("gmail_accounts").select("*").execute()
    
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
            
        # If under limit, return this account
        if count < 50:
            return account, count
            
    return None, 0  # No accounts available

def update_daily_count(gmail_account, count):
    """Update the daily count for an account"""
    today = date.today().isoformat()
    
    # Check if record exists
    existing = supabase.table("daily_email_counts") \
        .select("id") \
        .eq("gmail_account", gmail_account) \
        .eq("date", today) \
        .execute()
    
    if existing.data:
        # Update existing record
        supabase.table("daily_email_counts") \
            .update({"count": count}) \
            .eq("gmail_account", gmail_account) \
            .eq("date", today) \
            .execute()
    else:
        # Create new record
        supabase.table("daily_email_counts") \
            .insert({
                "gmail_account": gmail_account,
                "date": today,
                "count": count
            }) \
            .execute()

def send_queued():
    # Get queued emails that are scheduled for now or earlier
    queued = (
        supabase.table("email_queue")
        .select("*")
        .is_("sent_at", "null")
        .lte("scheduled_for", datetime.utcnow().isoformat())
        .limit(100)  # Increased limit to handle multiple accounts
        .execute()
    )

    if not queued.data:
        print("No queued emails ready to send.")
        return

    sent_count = 0
    failed_count = 0
    
    for q in queued.data:
        # Get an account with capacity
        account, current_count = get_account_with_capacity()
        if not account:
            print("All accounts have reached their daily limit (50 emails).")
            break
            
        try:
            access_token = get_gmail_access_token(account["encrypted_refresh_token"])

            message = build_email_message(
                sender=account["email"],
                to=q["lead_email"],
                subject=q["subject"],
                html_body=q["body"]
            )

            send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
            resp = requests.post(send_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }, json=message)

            if resp.status_code == 200:
                # Mark as sent (now including sent_from field)
                update_data = {
                    "sent_at": datetime.utcnow().isoformat(),
                    "message_id": resp.json().get("id"),
                    "sent_from": account["email"]  # This is the key addition
                }
                supabase.table("email_queue").update(update_data).match({"id": q["id"]}).execute()
                
                # Update daily count
                update_daily_count(account["email"], current_count + 1)
                
                # If this is an initial email (sequence 0), schedule the first follow-up
                if q["sequence"] == 0:
                    # Get the first follow-up for this campaign
                    follow_up = (
                        supabase.table("campaign_followups")
                        .select("*")
                        .eq("campaign_id", q["campaign_id"])
                        .eq("sequence", 1)
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
                            
                            # Queue follow-up
                            supabase.table("email_queue").insert({
                                "campaign_id": q["campaign_id"],
                                "lead_id": q["lead_id"],
                                "lead_email": q["lead_email"],
                                "subject": rendered_subject,
                                "body": rendered_body,
                                "sequence": 1,
                                "scheduled_for": send_date.isoformat()
                            }).execute()
                
                sent_count += 1
            else:
                print(f"Failed to send to {q['lead_email']}: {resp.text}")
                failed_count += 1
                
        except Exception as e:
            print(f"Error sending email to {q['lead_email']}: {str(e)}")
            failed_count += 1

    print(f"✅ Sent {sent_count} emails. Failed: {failed_count}")

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

if __name__ == "__main__":
    send_queued()
