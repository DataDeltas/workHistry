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

def save_to_db(data1, data2):
    if not data1 or not data1.get('issuccess') or not data2 or not data2.get('issuccess'):
        print("No data available to save.")
        return

    # Process data1 into combined DataFrame
    records1 = data1['data']
    df1 = pd.DataFrame(records1)
    df1 = df1[(df1[['totalPostQC', 'totalPostApproved', 'totalPostSkiped', 'totalPostReannotated',
                    'totalCommentQC', 'totalCommentApproved', 'totalCommentSkiped', 'totalCommentReannotated']] != 0).any(axis=1)]

    post_qc_df = df1[['date', 'totalPostQC', 'totalPostApproved', 'totalPostSkiped', 'totalPostReannotated']].rename(columns={
        'totalPostQC': 'total_qc_post_qc',
        'totalPostApproved': 'approved_post',
        'totalPostSkiped': 'skipped_post',
        'totalPostReannotated': 'reannotated_post'
    })

    comment_qc_df = df1[['date', 'totalCommentQC', 'totalCommentApproved', 'totalCommentSkiped', 'totalCommentReannotated']].rename(columns={
        'totalCommentQC': 'total_qc_comment_qc',
        'totalCommentApproved': 'approved_comment',
        'totalCommentSkiped': 'skipped_comment',
        'totalCommentReannotated': 'reannotated_comment'
    })

    combined_df = pd.merge(post_qc_df, comment_qc_df, on='date')

    # Get total work hour from data2
    records2 = data2['data']
    df2 = pd.DataFrame(records2)
    total_work_hour = df2['totalWorkHour'].iloc[0] if not df2.empty else 0

    # Add work hour to the row
    combined_df['total_work_hour'] = total_work_hour

    # Connect to SQLite database
    db_path = 'work_history.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{month_table_name}" (
            date TEXT PRIMARY KEY,
            total_qc_post_qc INTEGER,
            approved_post INTEGER,
            skipped_post INTEGER,
            reannotated_post INTEGER,
            total_qc_comment_qc INTEGER,
            approved_comment INTEGER,
            skipped_comment INTEGER,
            reannotated_comment INTEGER,
            total_work_hour REAL
        )
    ''')

    # Insert or update row for today
    row = combined_df.iloc[0] if not combined_df.empty else None
    if row is not None:
        cursor.execute(f'''
            INSERT OR REPLACE INTO "{month_table_name}" 
            (date, total_qc_post_qc, approved_post, skipped_post, reannotated_post, 
             total_qc_comment_qc, approved_comment, skipped_comment, reannotated_comment, total_work_hour)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['date'], row['total_qc_post_qc'], row['approved_post'], row['skipped_post'], row['reannotated_post'],
            row['total_qc_comment_qc'], row['approved_comment'], row['skipped_comment'], row['reannotated_comment'],
            row['total_work_hour']
        ))

    conn.commit()
    conn.close()
    print(f"Data saved to table {month_table_name} in {db_path} for {today}")

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

    data1 = fetch_qc_reports(cookie_jar, annotator_id)
    data2 = fetch_work_hours(cookie_jar, annotator_id)
    if data1 and data2:
        save_to_db(data1, data2)
    else:
        print("Failed to fetch data.")