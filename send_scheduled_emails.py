#!/usr/bin/env python3
import os
import smtplib
import json
import time
from email.mime.text import MIMEText
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# ── CONFIG ────────────────────────────────────────────────────────────────
MAX_DB_RETRIES = 3
RETRY_DELAY = 2
SMTP_TIMEOUT = 10

DB_URL = os.getenv("SUPABASE_DB_URL")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)

# ── DB CONNECTION HELPER ──────────────────────────────────────────────────
def get_db_connection():
    # Ensure SSL mode is set
    parsed = urlparse(DB_URL)
    query = parse_qs(parsed.query)
    if 'sslmode' not in query:
        DB_URL += "?sslmode=require" if '?' not in DB_URL else "&sslmode=require"
    
    for attempt in range(MAX_DB_RETRIES):
        try:
            conn = psycopg2.connect(
                dsn=DB_URL,
                connect_timeout=5,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10
            )
            return conn
        except psycopg2.OperationalError as e:
            if attempt == MAX_DB_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))
    raise Exception("Failed to connect to database after multiple attempts")

# ── SMTP HELPER ───────────────────────────────────────────────────────────
def get_smtp_connection():
    for attempt in range(MAX_DB_RETRIES):
        try:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            return server
        except Exception as e:
            if attempt == MAX_DB_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))

# ── MAIN EXECUTION ────────────────────────────────────────────────────────
def main():
    print(f"[{datetime.utcnow().isoformat()}] Starting email processing")
    
    try:
        # Database setup
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Fetch due emails
        cur.execute("""
        SELECT se.id AS job_id,
               se.type AS job_type,
               se.payload AS job_payload,
               l.id AS lead_id,
               l.email AS lead_email,
               l.name AS lead_name
        FROM public.scheduled_emails se
        JOIN public.leads l ON l.id = se.lead_id
        WHERE se.sent = false
          AND se.run_at <= NOW()
          AND l.unsubscribed = false
        ORDER BY se.run_at
        LIMIT 100  # Prevent overload
        """)
        jobs = cur.fetchall()

        if not jobs:
            print(f"[{datetime.utcnow().isoformat()}] No emails to send.")
            conn.close()
            return

        # SMTP setup
        server = get_smtp_connection()
        processed_count = 0

        for job in jobs:
            try:
                # Email content generation
                if job["job_type"] == "welcome":
                    subject = "Welcome to Our Service!"
                    html = f"""
                    <p>Hi {job['lead_name']},</p>
                    <p>Thanks for joining! We're excited to have you on board.</p>
                    <p>— The Team</p>
                    <p><a href="https://yourdomain.com/unsubscribe?id={job['lead_id']}">Unsubscribe</a></p>
                    """
                elif job["job_type"] == "feature_drop":
                    features = json.loads(job["job_payload"]).get("features", "new features")
                    subject = "Check Out Our New Features!"
                    html = f"""
                    <p>Hey {job['lead_name']},</p>
                    <p>We just launched: {features}</p>
                    <p><a href="https://yourdomain.com/unsubscribe?id={job['lead_id']}">Unsubscribe</a></p>
                    """
                else:
                    print(f"⚠️ Unknown job type: {job['job_type']}")
                    continue

                # Send email
                msg = MIMEText(html, "html")
                msg["Subject"] = subject
                msg["From"] = FROM_EMAIL
                msg["To"] = job["lead_email"]

                server.sendmail(FROM_EMAIL, [job["lead_email"]], msg.as_string())
                
                # Mark as sent
                cur.execute("""
                UPDATE public.scheduled_emails
                SET sent = true, sent_at = NOW()
                WHERE id = %s
                """, (job["job_id"],))
                conn.commit()
                
                processed_count += 1
                print(f"✅ Sent {job['job_type']} to {job['lead_email']}")

            except Exception as e:
                conn.rollback()
                print(f"❌ Failed to process job {job['job_id']}: {str(e)}")
                continue

        print(f"Processed {processed_count}/{len(jobs)} emails successfully")
        
    except Exception as e:
        print(f"🛑 Critical error: {str(e)}")
        raise
    finally:
        try:
            server.quit()
        except:
            pass
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()
