# check_replies.py
import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta
from app import supabase, aesgcm_decrypt

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
                            import re
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
                                # Mark this lead as responded
                                import requests
                                requests.post(
                                    "http://localhost:5000/api/leads/responded",
                                    json={"lead_id": lead.data[0]['id']},
                                    headers={"Content-Type": "application/json"}
                                )
                                
                                print(f"Marked lead {from_email} as responded")
            
            mail.close()
            mail.logout()
            
        except Exception as e:
            print(f"Error checking replies for {account['email']}: {str(e)}")

if __name__ == "__main__":
    check_for_replies()
