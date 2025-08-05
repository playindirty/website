#!/usr/bin/env python3
import smtplib
import json
import time
from email.mime.text import MIMEText
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ── HARDCODED CONFIGURATION ──────────────────────────────────────────────
DB_CONFIG = {
    "host": "db.ozbiubgszrvjdeogkvke.supabase.co",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "Lornav22@",
    "sslmode": "require",
    "options": "-c address_family=ipv4",  # Force IPv4 connection
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10
}

SMTP_CONFIG = {
    "host": "smtp.gmail.com",
    "port": 587,
    "timeout": 10,
    "user": "replyzeai@gmail.com",
    "password": "iwdl cbvy htyr bvod",
    "from_email": "replyzeai@gmail.com"
}

# ── CONSTANTS ────────────────────────────────────────────────────────────
MAX_RETRIES = 3
RETRY_DELAY = 2
UNSUBSCRIBE_URL = "https://yourdomain.com/unsubscribe?id={lead_id}"

# ── DB CONNECTION HELPER ─────────────────────────────────────────────────
def get_db_connection():
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES} to connect to database...")
            conn = psycopg2.connect(**DB_CONFIG)
            print("✅ Database connection successful")
            return conn
        except psycopg2.OperationalError as e:
            print(f"❌ Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise Exception(f"Database connection failed after {MAX_RETRIES} attempts")
            time.sleep(RETRY_DELAY * (attempt + 1))

# ── SMTP HELPER ──────────────────────────────────────────────────────────
def get_smtp_connection():
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES} to connect to SMTP...")
            server = smtplib.SMTP(
                host=SMTP_CONFIG["host"],
                port=SMTP_CONFIG["port"],
                timeout=SMTP_CONFIG["timeout"]
            )
            server.starttls()
            server.login(SMTP_CONFIG["user"], SMTP_CONFIG["password"])
            print("✅ SMTP connection successful")
            return server
        except Exception as e:
            print(f"❌ SMTP connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise Exception(f"SMTP connection failed after {MAX_RETRIES} attempts")
            time.sleep(RETRY_DELAY * (attempt + 1))

# ── EMAIL TEMPLATES ──────────────────────────────────────────────────────
def generate_welcome_email(lead_name, lead_id):
    return {
        "subject": "Welcome to Our Service!",
        "html": f"""
        <p>Hi {lead_name},</p>
        <p>Thanks for joining! We're excited to have you on board.</p>
        <p>— The Team</p>
        <p><a href="{UNSUBSCRIBE_URL.format(lead_id=lead_id)}">Unsubscribe</a></p>
        """
    }

def generate_feature_email(lead_name, lead_id, features):
    return {
        "subject": "Check Out Our New Features!",
        "html": f"""
        <p>Hey {lead_name},</p>
        <p>We just launched: {features}</p>
        <p><a href="{UNSUBSCRIBE_URL.format(lead_id=lead_id)}">Unsubscribe</a></p>
        """
    }

# ── MAIN EXECUTION ───────────────────────────────────────────────────────
def main():
    print(f"[{datetime.utcnow().isoformat()}] Starting email processing")
    
    conn = None
    server = None
    
    try:
        # Initialize connections
        conn = get_db_connection()
        server = get_smtp_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Fetch due emails
        cur.execute("""
        SELECT se.id, se.type, se.payload, 
               l.id as lead_id, l.email, l.name
        FROM public.scheduled_emails se
        JOIN public.leads l ON l.id = se.lead_id
        WHERE se.sent = false
          AND se.run_at <= NOW()
          AND l.unsubscribed = false
        ORDER BY se.run_at
        LIMIT 100
        """)
        jobs = cur.fetchall()

        if not jobs:
            print(f"[{datetime.utcnow().isoformat()}] No emails to send.")
            return

        processed_count = 0

        for job in jobs:
            try:
                # Generate email content
                if job["type"] == "welcome":
                    email_data = generate_welcome_email(job["name"], job["lead_id"])
                elif job["type"] == "feature_drop":
                    features = json.loads(job["payload"]).get("features", "new features")
                    email_data = generate_feature_email(job["name"], job["lead_id"], features)
                else:
                    print(f"⚠️ Unknown job type: {job['type']}")
                    continue

                # Send email
                msg = MIMEText(email_data["html"], "html")
                msg["Subject"] = email_data["subject"]
                msg["From"] = SMTP_CONFIG["from_email"]
                msg["To"] = job["email"]
                server.sendmail(SMTP_CONFIG["from_email"], [job["email"]], msg.as_string())
                
                # Mark as sent
                cur.execute("""
                UPDATE public.scheduled_emails
                SET sent = true, sent_at = NOW()
                WHERE id = %s
                """, (job["id"],))
                conn.commit()
                
                processed_count += 1
                print(f"✅ Sent {job['type']} to {job['email']}")

            except Exception as e:
                conn.rollback()
                print(f"❌ Failed to process job {job.get('id')}: {str(e)}")
                continue

        print(f"Processed {processed_count}/{len(jobs)} emails successfully")
        
    except Exception as e:
        print(f"🛑 Critical error: {str(e)}")
        raise
    finally:
        # Clean up resources
        if server:
            try:
                server.quit()
            except Exception as e:
                print(f"⚠️ Error closing SMTP connection: {str(e)}")
        
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"⚠️ Error closing database connection: {str(e)}")

if __name__ == "__main__":
    main()
