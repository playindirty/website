# check_replies.py
import os
import imaplib
import email
import base64
from email.header import decode_header
from datetime import datetime, timedelta
import re
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

def check_for_replies():
    # Get all SMTP accounts with IMAP configured
    accounts = supabase.table("smtp_accounts").select("*").not_.is_("imap_host", "null").execute()
    
    for account in accounts.data:
        try:
            # Connect to IMAP server
            mail = imaplib.IMAP4_SSL(account['imap_host'], account['imap_port'])
            mail.login(account['smtp_username'], aesgcm_decrypt(account['encrypted_smtp_password']))
            mail.select('inbox')
            
            # Search for unseen emails from the last 24 hours
            since_date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
            status, messages = mail.search(None, f'(UNSEEN SINCE {since_date})')
            email_ids = messages[0].split()
            
            for email_id in email_ids:
                # Fetch the email
                status, msg_data = mail.fetch(email_id, '(RFC822)')
                
                for response in msg_data:
                    if isinstance(response, tuple):
                        msg = email.message_from_bytes(response[1])
                        
                        # Check if this is a reply to one of our sent emails
                        subject = decode_header(msg["Subject"])[0][0]
                        if isinstance(subject, bytes):
                            subject = subject.decode()
                        
                        # Check if this email is a reply (starts with "Re:")
                        if subject.lower().startswith("re:"):
                            from_email = msg.get("From")
                            
                            # Extract email address from the From field
                            email_match = re.search(r'<(.+?)>', from_email)
                            if email_match:
                                from_email = email_match.group(1)
                            else:
                                # If no angle brackets, try to extract email directly
                                email_match = re.search(r'[\w\.-]+@[\w\.-]+', from_email)
                                if email_match:
                                    from_email = email_match.group(0)
                            
                            # Find the lead by email
                            lead = supabase.table("leads").select("*").eq("email", from_email).execute()
                            
                            if lead.data:
                                # Copy the lead to responded_leads table
                                supabase.table("responded_leads").insert({
                                    "original_lead_id": lead.data[0]['id'],
                                    "email": lead.data[0]['email'],
                                    "name": lead.data[0]['name'],
                                    "last_name": lead.data[0].get('last_name'),
                                    "city": lead.data[0].get('city'),
                                    "brokerage": lead.data[0].get('brokerage'),
                                    "service": lead.data[0].get('service'),
                                    "list_name": lead.data[0].get('list_name'),
                                    "custom_fields": lead.data[0].get('custom_fields')
                                }).execute()
                                
                                # Delete any queued emails for this lead
                                supabase.table("email_queue").delete().eq("lead_id", lead.data[0]['id']).execute()
                                
                                # Remove any account assignments for this lead
                                supabase.table("lead_campaign_accounts").delete().eq("lead_id", lead.data[0]['id']).execute()
                                
                                # Mark the lead as responded in the leads table (don't delete it)
                                supabase.table("leads").update({
                                    "responded": True,
                                    "responded_at": datetime.now().isoformat()
                                }).eq("id", lead.data[0]['id']).execute()
                                
                                print(f"Marked lead {from_email} as responded")
            
            mail.close()
            mail.logout()
            
        except Exception as e:
            print(f"Error checking replies for {account['email']}: {str(e)}")

if __name__ == "__main__":
    check_for_replies()
