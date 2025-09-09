# reset_daily_counts.py
from datetime import date
from app import supabase

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
