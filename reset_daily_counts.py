# reset_daily_counts.py
import os
from datetime import date
from supabase import create_client

# Initialize Supabase
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def reset_daily_counts():
    today = date.today().isoformat()
    
    # Check if we've already reset counts today
    existing = supabase.table("daily_email_counts") \
        .select("id") \
        .eq("date", today) \
        .execute()
    
    if not existing.data:
        # Reset counts for all accounts
        accounts = supabase.table("smtp_accounts").select("email").execute()
        
        for account in accounts.data:
            supabase.table("daily_email_counts") \
                .insert({
                    "email_account": account["email"],
                    "date": today,
                    "count": 0
                }) \
                .execute()
        
        print("Reset daily email counts for all accounts")
    else:
        print("Daily counts already reset today")

if __name__ == "__main__":
    reset_daily_counts()
