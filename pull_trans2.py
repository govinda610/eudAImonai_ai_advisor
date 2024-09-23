import emails.authenticate as auth
import emails.read_modify_emails as rdmd
from datetime import datetime, date
import base64
import csv
import sqlite3
import re
from bs4 import BeautifulSoup

# Define the CSV file and database name
CSV_FILE = 'trans.csv'
DB_FILE = 'trans.db'

# Get today's date and two years ago
today = date.today().strftime("%d/%m/%Y")
old_dt = date(date.today().year - 2, date.today().month, date.today().day).strftime("%d/%m/%Y")

# Connect to the database
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Create the emails table if it doesn't exist
c.execute('''CREATE TABLE IF NOT EXISTS trans
             (id TEXT PRIMARY KEY, datetime TEXT, to_email TEXT, from_email TEXT, subject TEXT, 
              transaction_amount REAL, transaction_account_type TEXT, transaction_account_number TEXT, 
              transaction_with TEXT, transaction_type TEXT, transaction_date TEXT, 
              is_upi INTEGER, reference_number TEXT, email_text TEXT)''')

# Function to get the last pull date from the database and convert it to the required format
def get_last_pull_date():
    try:
        c.execute('SELECT MAX(datetime) FROM trans')
        last_date = c.fetchone()[0]
        if last_date:
            last_date = datetime.strptime(last_date, '%Y-%m-%d').strftime('%d/%m/%Y')
        else:
            last_date = old_dt
        return last_date
    except Exception as e:
        print(f"Error getting last pull date: {e}")
        return old_dt

# Function to save email data into the database and CSV
def save_to_db_and_csv(email_data):
    # Check if CSV file is empty to add header
    with open(CSV_FILE, 'a', newline='') as file:
        writer = csv.writer(file)
        # Add header if file is empty
        if file.tell() == 0:
            writer.writerow(['ID', 'Date', 'To Email', 'From Email', 'Subject', 
                             'Transaction Amount', 'Transaction Account Type', 
                             'Transaction Account Number', 'Transaction With', 
                             'Transaction Type', 'Transaction Date', 
                             'Is UPI', 'Reference Number', 'Email Text'])
        writer.writerow(email_data)

    try:
        c.execute('INSERT OR IGNORE INTO trans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', email_data)
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # Ignore duplicates

import re

def extract_transaction_details(body, subject, email_date):
    patterns = {
        'savings_account': r'(?:Rs\.?|INR)\s*([,\d]+(?:\.\d{1,2})?) (?:has been|is successfully) (debited|credited|withdrawn|deposited) (?:from|to) (?:your account|account) (?:\*\*|XX)(\d{4}) (?:by|to|from) (.+?) on (\d{2}-\d{2}-\d{2,4})',
        'rupay_credit_card': r'(?:Rs\.?|INR)\s*([,\d]+(?:\.\d{1,2})?) has been (debited|credited|withdrawn|deposited) from your HDFC Bank RuPay Credit Card (?:XX|)(\d{4}) (?:to|by|at) (.+?) on (\d{2}-\d{2}-\d{2,4})',
        'upi_transaction': r'(?:Rs\.?|INR)\s*([,\d]+(?:\.\d{2})?) is (?:successfully )?(credited|debited|withdrawn|deposited) (?:to|from) your account \*\*(\d{4}) by (.+?) on (\d{2}-\d{2}-\d{2,4})\. Your UPI transaction reference number is (\d+)',
        'hdfc_credit_card': r'Thank you for using (?:your )?HDFC Bank (?:Credit )?Card (?:ending |XX)(\d{4}) for (?:Rs\.?|INR)\s*([,\d]+(?:\.\d{1,2})?) at (.+?) on (\d{2}-\d{2}-\d{2,4})',
        'debit_card_transaction': r'Thank you for using your HDFC Bank Debit Card ending (\d{4}) for (?:Rs\.?|INR)\s*([,\d]+(?:\.\d{1,2})?) at (.+?) on (\d{2}-\d{2}-\d{2,4})',
        'withdrawal_alert': r'The amount (?:debited|withdrawn) is (?:Rs\.?|INR)\s*([\d,]+(?:\.\d{1,2})?) from your account XX(\d{4}) on (\d{2}-[A-Z]{3}-\d{4}) (?:on account of|by) (.+?)(?:\.|$)',
        'new_deposit_alert': r'The amount credited/received is (?:Rs\.?|INR)\s*([\d,]+(?:\.\d{1,2})?) in your account XX(\d{4}) on (\d{2}-[A-Z]{3}-\d{4}) on account of (.+?)(?:\.|$)',
        'emi_debit': r'The amount debited/drawn is (?:Rs\.?|INR)\s*([\d,]+(?:\.\d{1,2})?) from your account XX(\d{4}) on (\d{2}-[A-Z]{3}-\d{4}) on account of (.+?)(?:\.|$)',
        'withdrawal_alert_new': r'The amount debited/drawn is\s+(?:Rs\.?|INR)\s*([\d,]+(?:\.\d{1,2})?)\s+from your account XX(\d{4}) on (\d{2}-[A-Z]{3}-\d{4}) on account of (.+?)(?=Your A/c is at|$)',
        'account_update': r'(?:Rs\.?|INR)\s*([,\d]+(?:\.\d{1,2})?) (?:has been|is successfully) (debited|credited|withdrawn|deposited) (?:from|to) your account (?:No\. )?(?:XXXX|XX)(\d{4})(?: on account of | by )(.+?)(?:\.|$)',
        'atm_withdrawal': r'Thank you for using your HDFC Bank Credit Card ending (\d{4}) for ATM withdrawal for (?:Rs\.?|INR)\s*([,\d]+(?:\.\d{1,2})?) in (.+?) at (.+?) on (\d{2}-\d{2}-\d{2,4})',
        'imps_transaction': r'The amount debited/drawn is\s+(?:Rs\.?|INR)\s*([\d,]+(?:\.\d{1,2})?)\s+from your account XX(\d{4}) on (\d{2}-[A-Z]{3}-\d{4}) on account of (IMPS-\d+-[^-]+-[^-]+-[^-]+-[^-]+)(?=Your A/c is at|$)',
    }

    transaction_details = {}
    max_fields_filled = 0

    for account_type, pattern in patterns.items():
        match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        if match:
            details = {}
            filled_fields = 0
            
            try:
                if account_type in ['savings_account', 'rupay_credit_card', 'upi_transaction']:
                    details['transaction_amount'] = float(match.group(1).replace(',', ''))
                    details['transaction_account_number'] = match.group(3)
                    details['transaction_with'] = match.group(4)
                    details['transaction_date'] = match.group(5)
                    details['transaction_type'] = 'debit' if 'debited' in match.group(2) else 'credit'
                    details['transaction_account_type'] = 'savings' if '3608' in match.group(3) else 'credit'
                    filled_fields = len(details)

                elif account_type in ['hdfc_credit_card', 'debit_card_transaction']:
                    details['transaction_account_number'] = match.group(1)
                    details['transaction_amount'] = float(match.group(2).replace(',', ''))
                    details['transaction_with'] = match.group(3)
                    details['transaction_date'] = match.group(4)
                    details['transaction_type'] = 'debit'
                    details['transaction_account_type'] = 'savings' if '3608' in match.group(1) else 'credit'
                    filled_fields = len(details)

                elif account_type in ['withdrawal_alert', 'new_deposit_alert', 'emi_debit', 'withdrawal_alert_new']:
                    details['transaction_amount'] = float(match.group(1).replace(',', ''))
                    details['transaction_account_number'] = match.group(2)
                    details['transaction_date'] = match.group(3)
                    details['transaction_with'] = match.group(4)
                    details['transaction_type'] = 'debit' if account_type != 'new_deposit_alert' else 'credit'
                    details['transaction_account_type'] = 'savings' if '3608' in match.group(2) else 'credit'
                    filled_fields = len(details)

                elif account_type == 'account_update':
                    details['transaction_amount'] = float(match.group(1).replace(',', ''))
                    details['transaction_account_number'] = match.group(3)
                    details['transaction_with'] = match.group(4)
                    details['transaction_type'] = 'debit' if 'debited' in match.group(2) else 'credit'
                    details['transaction_account_type'] = 'savings' if '3608' in match.group(3) else 'credit'
                    filled_fields = len(details)

                elif account_type == 'atm_withdrawal':
                    details['transaction_account_number'] = match.group(1)
                    details['transaction_amount'] = float(match.group(2).replace(',', ''))
                    details['transaction_with'] = match.group(3)
                    details['transaction_date'] = match.group(5)
                    details['transaction_type'] = 'debit'
                    details['transaction_account_type'] = 'savings' if '3608' in match.group(1) else 'credit'
                    filled_fields = len(details)

                elif account_type == 'imps_transaction':
                    details['transaction_amount'] = float(match.group(1).replace(',', ''))
                    details['transaction_account_number'] = match.group(2)
                    details['transaction_date'] = match.group(3)
                    details['transaction_with'] = match.group(4)
                    details['transaction_type'] = 'debit'
                    details['transaction_account_type'] = 'savings' if '3608' in match.group(2) else 'credit'
                    filled_fields = len(details)

                if 'transaction_date' in details:
                    day, month, year = details['transaction_date'].split('-')
                    if len(year) == 2:
                        year = '20' + year
                    month_map = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 
                                 'JUN': '06', 'JUL': '07', 'AUG': '08', 'SEP': '09', 
                                 'OCT': '10', 'NOV': '11', 'DEC': '12'}
                    if month in month_map:
                        month = month_map[month.upper()]
                    details['transaction_date'] = f"{year}-{month}-{day}"

                if filled_fields > max_fields_filled:
                    max_fields_filled = filled_fields
                    transaction_details = details

            except (ValueError, IndexError) as e:
                print(f"Error processing {account_type}: {e}")

    if not transaction_details:
        print(f"No match found for email with subject: {subject}")
        print(f"Email body: {body[:500]}...")

    return transaction_details



# Function to fetch and process emails
def get_trans(after=None, from_='alerts@hdfcbank.net'):
    if after is None:
        after = get_last_pull_date()

    # Filter for specific subjects
    subjects = [
        "You have done a UPI txn. Check details!",
        "View: Account update for your HDFC Bank A/c",
        "Withdrawal Alert: Check your A/c balance now!",
        "New Deposit Alert: Check your A/c balance now!",
        "Alert : Update on your HDFC Bank Credit Card"
    ]
    subjects_query = ' OR '.join([f'subject:"{subject}"' for subject in subjects])

    service = auth.gmail_authenticate()
    query = f"from:{from_} AND before:{today} AND after:{after} AND ({subjects_query})"
    print(f"Searching emails with query: {query}")
    results = rdmd.search_messages(service, query, num_res=3000)

    if not results:
        print("No messages found.")
        return

    for msg in results:
        try:
            txt = service.users().messages().get(userId='me', id=msg['id']).execute()
            payload = txt.get('payload', {})
            headers = payload.get('headers', [])
            subject, sender, to_email = None, None, None
            
            for d in headers:
                if d['name'] == 'Subject':
                    subject = d['value']
                if d['name'] == 'From':
                    sender = d['value']
                if d['name'] == 'To':
                    to_email = d['value']

            body = ""
            if 'parts' in payload:
                for part in payload['parts']:
                    mime_type = part['mimeType']
                    if mime_type in ['text/plain', 'text/html']:
                        data = part['body']['data']
                        decoded_data = base64.urlsafe_b64decode(data)
                        soup = BeautifulSoup(decoded_data, "lxml")
                        body += soup.get_text()
            
            # Extract transaction details
            email_date = datetime.fromtimestamp(int(txt['internalDate']) / 1000).strftime('%d/%m/%Y')  # Convert timestamp to date
            transaction_details = extract_transaction_details(body, subject, email_date)

            # Prepare data for saving
            email_data = (msg['id'], today, to_email, sender, subject,
                          transaction_details.get('transaction_amount', None),
                          transaction_details.get('transaction_account_type', None),
                          transaction_details.get('transaction_account_number', None),
                          transaction_details.get('transaction_with', None),
                          transaction_details.get('transaction_type', None),
                          transaction_details.get('transaction_date', None),
                          transaction_details.get('is_upi', 1 if 'UPI' in subject else 0),
                          transaction_details.get('reference_number', None),
                          body)  # Store email text
            
            # Save the email to both CSV and database, ensuring no duplicates
            save_to_db_and_csv(email_data)

        except Exception as e:
            print(f"Error occurred: {e}")

# Fetch new emails and save
get_trans()

# Close the database connection
conn.close()