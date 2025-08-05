import os
import asyncio
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

# Config
DB_URL = os.getenv('SUPABASE_DB_URL')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
FROM_EMAIL = os.getenv('FROM_EMAIL', SMTP_USERNAME)

app = FastAPI()

# Pydantic models
type LeadCreate = BaseModel
class LeadCreate(BaseModel):
    name: str
    email: str

# DB helper
def get_db_connection(autocommit=False, max_retries=3, retry_delay=2):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dsn=DB_URL,
                sslmode="require",
                connect_timeout=10
            )
            if autocommit:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn
        except psycopg2.OperationalError as e:
            if attempt == max_retries - 1:
                raise
            print(f"Retrying DB connection (attempt {attempt + 1})...")
            time.sleep(retry_delay)

# Signup endpoint\@app.post('/signup')
async def signup(lead: LeadCreate):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            'INSERT INTO public.leads (name, email) VALUES (%s, %s) RETURNING id',
            (lead.name, lead.email)
        )
        lead_id = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail='DB error')
    finally:
        cur.close()
        conn.close()
    return { 'id': lead_id }

# Unsubscribe endpoint\@app.get('/unsubscribe')
async def unsubscribe(id: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        'UPDATE public.leads SET unsubscribed = true WHERE id = %s',
        (id,)
    )
    conn.commit()
    cur.close()
    conn.close()
    return { 'status': 'unsubscribed' }

# Email sender
def send_email(to_email: str, subject: str, html_content: str):
    msg = MIMEText(html_content, 'html')
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = to_email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, to_email, msg.as_string())

# Handle new leads
def schedule_welcome(lead_id: str):
    async def _task():
        await asyncio.sleep(300)  # 5 minutes
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'SELECT name, email, unsubscribed FROM public.leads WHERE id = %s',
            (lead_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row or row['unsubscribed']:
            return
        subject = 'Welcome!'
        html = f"""
            <p>Hi {row['name']},</p>
            <p>Thanks for joining us!</p>
            <p><a href=\"/unsubscribe?id={lead_id}\">Unsubscribe</a></p>
        """
        send_email(row['email'], subject, html)
    asyncio.create_task(_task())

# Listen for notifications
async def listen_new_leads():
    conn = get_db_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("LISTEN new_lead_channel;")
    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            schedule_welcome(notify.payload)
        await asyncio.sleep(1)

@app.on_event('startup')
async def startup():
    asyncio.create_task(listen_new_leads())

@app.get('/')
async def root():
    return { 'status': 'ok', 'time': datetime.utcnow().isoformat() }
