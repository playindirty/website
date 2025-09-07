# reset_daily_counts.py (run this daily via cron)
from app import supabase
from datetime import date

def reset_daily_counts():
    # Delete all records from daily_email_counts
    # We'll recreate them as needed
    supabase.table("daily_email_counts").delete().neq("id", 0).execute()
    print("Daily email counts reset.")

if __name__ == "__main__":
    reset_daily_counts()
