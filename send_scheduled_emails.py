#!/usr/bin/env python3
import os, smtplib, json
from email.mime.text import MIMEText
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────
DB_URL        = os.getenv("SUPABASE_DB_URL")
SMTP_HOST     = os.getenv("SMTP_HOST")       # e.g. "smtp.mail.yourcompany.com"
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER")
SMTP_PASS     = os.getenv("SMTP_PASS")
FROM_EMAIL    = os.getenv("FROM_EMAIL", SMTP_USER)

# ── CONNECT DB ─────────────────────────────────────────────────────────────
conn = psycopg2.connect(DB_URL)
cur  = conn.cursor(cursor_factory=RealDictCursor)

# ── FETCH DUE EMAILS ───────────────────────────────────────────────────────
cur.execute("""
SELECT se.id      AS job_id,
       se.type    AS job_type,
       se.payload AS job_payload,
       l.id       AS lead_id,
       l.email    AS lead_email,
       l.name     AS lead_name
FROM public.scheduled_emails se
JOIN public.leads l ON l.id = se.lead_id
WHERE se.sent = false
  AND se.run_at <= NOW()
  AND l.unsubscribed = false;
""")
jobs = cur.fetchall()

if not jobs:
    print(f"[{datetime.utcnow().isoformat()}] No emails to send.")
    conn.close()
    exit(0)

# ── SMTP SETUP ─────────────────────────────────────────────────────────────
server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
server.starttls()
server.login(SMTP_USER, SMTP_PASS)

# ── SEND LOOP ──────────────────────────────────────────────────────────────
for job in jobs:
    subject = ""
    html    = ""
    # Customize per job type
    if job["job_type"] == "welcome":
        subject = "Welcome to Our Service!"
        html = f"""
          <p>Hi {job['lead_name']},</p>
          <p>Thanks for joining! We’re excited to have you on board.</p>
          <p>— The Team</p>
          <p>
            <a href="https://yourdomain.com/unsubscribe?id={job['lead_id']}">
              Unsubscribe
            </a>
          </p>
        """
    elif job["job_type"] == "feature_drop":
        subject = "Check Out Our New Features!"
        html = f"""
          <p>Hey {job['lead_name']},</p>
          <p>We just launched some cool stuff: {job['job_payload'].get('features')}</p>
          <p>
            <a href="https://yourdomain.com/unsubscribe?id={job['lead_id']}">
              Unsubscribe
            </a>
          </p>
        """
    else:
        continue  # unknown job type

    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"]    = FROM_EMAIL
    msg["To"]      = job["lead_email"]

    try:
        server.sendmail(FROM_EMAIL, [job["lead_email"]], msg.as_string())
        # mark sent
        cur.execute("""
          UPDATE public.scheduled_emails
          SET sent = true, sent_at = NOW()
          WHERE id = %s
        """, (job["job_id"],))
        conn.commit()
        print(f"✅ Sent '{job['job_type']}' to {job['lead_email']}")
    except Exception as e:
        print(f"❌ Error sending to {job['lead_email']}: {e}")

server.quit()
conn.close()
