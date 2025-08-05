import os
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Config (same as before)
DB_URL = os.getenv('SUPABASE_DB_URL')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
FROM_EMAIL = os.getenv('FROM_EMAIL', SMTP_USERNAME)

# DB helper (same as before)
def get_db_connection(autocommit=False):
    conn = psycopg2.connect(
        dsn=DB_URL,
        sslmode="require",
        connect_timeout=10
    )
    if autocommit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

# Routes
@app.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            'INSERT INTO public.leads (name, email) VALUES (%s, %s) RETURNING id',
            (data['name'], data['email'])
        )
        lead_id = cur.fetchone()[0]
        conn.commit()
        # Schedule welcome email
        threading.Thread(target=schedule_welcome, args=(lead_id,)).start()
        return jsonify({'id': lead_id})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': 'DB error'}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/unsubscribe', methods=['GET'])
def unsubscribe():
    lead_id = request.args.get('id')
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            'UPDATE public.leads SET unsubscribed = true WHERE id = %s',
            (lead_id,)
        )
        conn.commit()
        return jsonify({'status': 'unsubscribed'})
    finally:
        cur.close()
        conn.close()

@app.route('/')
def root():
    return jsonify({'status': 'ok', 'time': datetime.utcnow().isoformat()})

# Email functions (same as before)
def send_email(to_email, subject, html_content):
    msg = MIMEText(html_content, 'html')
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = to_email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, to_email, msg.as_string())

def schedule_welcome(lead_id):
    time.sleep(300)  # 5 minutes delay
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            'SELECT name, email, unsubscribed FROM public.leads WHERE id = %s',
            (lead_id,)
        )
        row = cur.fetchone()
        if not row or row['unsubscribed']:
            return
        subject = 'Welcome!'
        html = f"""
            <p>Hi {row['name']},</p>
            <p>Thanks for joining us!</p>
            <p><a href="/unsubscribe?id={lead_id}">Unsubscribe</a></p>
        """
        send_email(row['email'], subject, html)
    finally:
        cur.close()
        conn.close()

# Database listener thread
def listen_new_leads():
    while True:
        try:
            conn = get_db_connection(autocommit=True)
            cur = conn.cursor()
            cur.execute("LISTEN new_lead_channel;")
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                threading.Thread(target=schedule_welcome, args=(notify.payload,)).start()
            time.sleep(1)
        except Exception as e:
            print(f"Database listener error: {e}")
            time.sleep(5)

# Start background thread when app runs
if __name__ == '__main__':
    threading.Thread(target=listen_new_leads, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
