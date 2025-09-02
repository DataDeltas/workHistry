import httpx
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import os

# Get today's date and month for table name
today = datetime.now().strftime('%Y-%m-%d')
month_table_name = datetime.now().strftime('%B_%Y')  # e.g., September_2025

# Constants
LOGIN_URL = "https://roobtech.com/Account/Login"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'X-Requested-With': 'XMLHttpRequest',
    'DNT': '1',
    'Sec-GPC': '1',
    'Connection': 'keep-alive',
}

def login(email, password):
    try:
        with httpx.Client(follow_redirects=True) as client:
            # Get login page to extract CSRF token
            login_page = client.get(LOGIN_URL, headers=HEADERS)
            soup = BeautifulSoup(login_page.text, 'html.parser')
            csrf_token = soup.find('input', {'name': '__RequestVerificationToken'})
            if not csrf_token:
                print("Failed to find CSRF token on login page")
                return None
            login_data = {
                "Email": email,
                "Password": password,
                "RememberMe": "true",
                "__RequestVerificationToken": csrf_token['value']
            }
            # Make login request
            response = client.post(LOGIN_URL, data=login_data, headers=HEADERS)
            if response.status_code == 200 and "Login" not in response.url.path:
                print("Login successful")
                return client.cookies.jar
            else:
                print(f"Login failed. Status code: {response.status_code}, URL: {response.url}")
                return None
    except httpx.HTTPError as e:
        print(f"Error during login: {e}")
        return None

def fetch_qc_reports(cookie_jar, annotator_id):
    url = 'https://roobtech.com/ProjectReport/GetQCHourWiseReports'
    params = {
        'projectId': 'all',
        'annotatorId': annotator_id,
        'fromdate': f"{today} 00:00",
        'todate': f"{today} 23:55",
        'sortby': 'day',
        'type': 'annotated',
        'interactionType': '2'
    }
    try:
        cookies = {cookie.name: cookie.value for cookie in cookie_jar}
        response = requests.get(url, headers=HEADERS, params=params, cookies=cookies)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching QC reports: {e}")
        return None

def fetch_work_hours(cookie_jar, annotator_id):
    url = 'https://roobtech.com/WorkingHour/GetQCWorkingHourReports'
    params = {
        'type': '1',
        'fromdate': f"{today} 00:00",
        'todate': f"{today} 23:55",
        'projectId': 'all',
        'annotatorId': annotator_id
    }
    try:
        cookies = {cookie.name: cookie.value for cookie in cookie_jar}
        response = requests.get(url, headers=HEADERS, params=params, cookies=cookies)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching work hours: {e}")
        return None

def save_to_db(qc_data, work_data):
    if not qc_data or not qc_data.get('issuccess'):
        print("No QC data available to save.")
        return
    
    if not work_data or not work_data.get('issuccess'):
        print("No work hours data available to save.")
        return

    # Get total work hours for the day
    work_records = work_data['data']
    total_work_hours = 0
    if work_records:
        work_df = pd.DataFrame(work_records)
        total_work_hours = work_df['totalWorkHour'].iloc[0] if not work_df.empty else 0

    # Process hourly QC data
    qc_records = qc_data['data']
    
    # Connect to SQLite database
    db_path = 'work_history.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists - better structure
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{month_table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            hour TEXT NOT NULL,
            post_qc INTEGER DEFAULT 0,
            post_approved INTEGER DEFAULT 0,
            post_skipped INTEGER DEFAULT 0,
            post_reannotated INTEGER DEFAULT 0,
            comment_qc INTEGER DEFAULT 0,
            comment_approved INTEGER DEFAULT 0,
            comment_skipped INTEGER DEFAULT 0,
            comment_reannotated INTEGER DEFAULT 0,
            UNIQUE(date, hour)
        )
    ''')

    # Create daily summary table
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{month_table_name}_daily_summary" (
            date TEXT PRIMARY KEY,
            total_post_qc INTEGER DEFAULT 0,
            total_post_approved INTEGER DEFAULT 0,
            total_post_skipped INTEGER DEFAULT 0,
            total_post_reannotated INTEGER DEFAULT 0,
            total_comment_qc INTEGER DEFAULT 0,
            total_comment_approved INTEGER DEFAULT 0,
            total_comment_skipped INTEGER DEFAULT 0,
            total_comment_reannotated INTEGER DEFAULT 0,
            total_work_hours REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Clear existing data for today
    cursor.execute(f'DELETE FROM "{month_table_name}" WHERE date = ?', (today,))

    # Insert hourly data (only non-zero hours)
    daily_totals = {
        'post_qc': 0, 'post_approved': 0, 'post_skipped': 0, 'post_reannotated': 0,
        'comment_qc': 0, 'comment_approved': 0, 'comment_skipped': 0, 'comment_reannotated': 0
    }

    hours_worked = 0
    
    for record in qc_records:
        hour = record['date']  # This is actually the hour like "07:00 AM"
        
        # Extract numeric values
        post_qc = int(record['totalPostQC'])
        post_approved = int(record['totalPostApproved'])
        post_skipped = int(record['totalPostSkiped'])
        post_reannotated = int(record['totalPostReannotated'])
        comment_qc = int(record['totalCommentQC'])
        comment_approved = int(record['totalCommentApproved'])
        comment_skipped = int(record['totalCommentSkiped'])
        comment_reannotated = int(record['totalCommentReannotated'])
        
        # Only save hours where there was actual work
        if any([post_qc, post_approved, post_skipped, post_reannotated, 
                comment_qc, comment_approved, comment_skipped, comment_reannotated]):
            
            cursor.execute(f'''
                INSERT OR REPLACE INTO "{month_table_name}" 
                (date, hour, post_qc, post_approved, post_skipped, post_reannotated,
                 comment_qc, comment_approved, comment_skipped, comment_reannotated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, hour, post_qc, post_approved, post_skipped, post_reannotated,
                  comment_qc, comment_approved, comment_skipped, comment_reannotated))
            
            hours_worked += 1
            
        # Add to daily totals
        daily_totals['post_qc'] += post_qc
        daily_totals['post_approved'] += post_approved
        daily_totals['post_skipped'] += post_skipped
        daily_totals['post_reannotated'] += post_reannotated
        daily_totals['comment_qc'] += comment_qc
        daily_totals['comment_approved'] += comment_approved
        daily_totals['comment_skipped'] += comment_skipped
        daily_totals['comment_reannotated'] += comment_reannotated

    # Insert daily summary
    cursor.execute(f'''
        INSERT OR REPLACE INTO "{month_table_name}_daily_summary" 
        (date, total_post_qc, total_post_approved, total_post_skipped, total_post_reannotated,
         total_comment_qc, total_comment_approved, total_comment_skipped, total_comment_reannotated, total_work_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (today, daily_totals['post_qc'], daily_totals['post_approved'], 
          daily_totals['post_skipped'], daily_totals['post_reannotated'],
          daily_totals['comment_qc'], daily_totals['comment_approved'],
          daily_totals['comment_skipped'], daily_totals['comment_reannotated'], 
          total_work_hours))

    conn.commit()
    conn.close()
    
    print(f"✅ Data saved successfully!")
    print(f"📅 Date: {today}")
    print(f"⏰ Hours worked: {hours_worked}")
    print(f"📊 Total Post QC: {daily_totals['post_qc']}")
    print(f"📊 Total Comment QC: {daily_totals['comment_qc']}")
    print(f"🕐 Total work hours: {total_work_hours}")
    print(f"💾 Database: work_history.db")

if __name__ == "__main__":
    # Load credentials from environment variables
    email = os.environ.get('EMAIL')
    password = os.environ.get('PASSWORD')
    annotator_id = os.environ.get('ANNOTATOR_ID')

    if not all([email, password, annotator_id]):
        print("Missing environment variables for credentials.")
        exit(1)

    cookie_jar = login(email, password)
    if not cookie_jar:
        print("Failed to obtain cookies. Exiting.")
        exit(1)

    qc_data = fetch_qc_reports(cookie_jar, annotator_id)
    work_data = fetch_work_hours(cookie_jar, annotator_id)
    
    if qc_data and work_data:
        save_to_db(qc_data, work_data)
    else:
        print("Failed to fetch data.")
