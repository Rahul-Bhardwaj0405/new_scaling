import pandas as pd
import logging
from io import StringIO, BytesIO
from .models import BookingData, RefundData
from celery import shared_task
from django.db import connection
import json
from pyexcel_ods import get_data as ods_get_data
import re


# For Oracle DB connection
# import cx_Oracle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Bank name to code mapping (can be stored in DB)
BANK_CODE_MAPPING = {
    'hdfc': 101,
    'icici': 102,
    'karur_vysya': 40,
}


# Bank-specific mappings for booking and refund files
BANK_MAPPINGS = {
    'hdfc': {
        'booking': {
            'columns': ['IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT'],
            'column_mapping': {
                'IRCTC ORDER NO.': 'irctc_order_no',
                'BANK BOOKING REF.NO.': 'bank_booking_ref_no',
                'BOOKING AMOUNT': 'sale_amount'
            }
        },
        'refund': {
            'columns': ['REFUND ORDER NO.', 'REFUND AMOUNT', 'CREDITED ON'],
            'column_mapping': {
                'REFUND ORDER NO.': 'irctc_order_no',
                'REFUND AMOUNT': 'sale_amount',
                'CREDITED ON': 'date'
            }
        }
    },
    'icici': {
        'booking': {
            'columns': ['ORDER NO.', 'REFERENCE NO.', 'AMOUNT'],
            'column_mapping': {
                'ORDER NO.': 'irctc_order_no',
                'REFERENCE NO.': 'bank_booking_ref_no',
                'AMOUNT': 'sale_amount',
                'BRANCH CODE': 'branch_code',
                'OTHER COLUMN': 'other_column'
            }
        },
        'refund': {
            'columns': ['ORDER NO.', 'REFUND AMOUNT', 'CREDIT DATE'],
            'column_mapping': {
                'ORDER NO.': 'irctc_order_no',
                'REFUND AMOUNT': 'sale_amount',
                'CREDIT DATE': 'date',
                'BRANCH CODE': 'branch_code',
                'OTHER COLUMN': 'other_column'
            }
        }
    },

'karur_vysya': {
    'booking': {
        'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
        'column_mapping': {
            'TXNDATE': 'txn_date',
            'IRCTCORDERNO': 'irctc_order_no',
            'BANKBOOKINGREFNO': 'bank_booking_ref_no',
            'BOOKINGAMOUNT': 'booking_amount',
            'CREDITEDON': 'credited_date'
        }
    },
    'refund': {
        'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
        'column_mapping': {
            'REFUNDDATE': 'refund_date',
            'IRCTCORDERNO': 'irctc_order_no',
            'BANKBOOKINGREFNO': 'bank_booking_ref_no',
            'BANKREFUNDREFNO': 'bank_refund_ref_no',
            'REFUNDAMOUNT': 'refund_amount',
            'DEBITEDON': 'debited_date'
        }
    }
},

    # Add more bank mappings as needed
}

# Date parsing helper function with logging
def try_parse_date(date_str):
    if pd.isnull(date_str) or date_str.strip() == '':
        logging.warning(f"Empty or null date string found: {date_str}")
        return pd.NaT

    date_str = date_str.strip()  # Trim any leading/trailing whitespace
    logging.info(f"Attempting to parse date string: {date_str}")
    
    # Try specific formats first
    for fmt in ('%d-%b-%y', '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%m/%d/%Y'):  # Add more formats as needed
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue

    # If specific formats fail, use a general approach with coercion
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except Exception as e:
        logging.error(f"Unable to parse date string: {date_str}. Error: {e}")
        return pd.NaT

@shared_task
def process_uploaded_files(file_content, file_name, bank_name, transaction_type):
    logging.info(f"Starting to process file: {file_name} for bank: {bank_name}, transaction type: {transaction_type}")

    try:
        df = pd.DataFrame()

        # Set possible delimiters for CSV and text files
        possible_delimiters = [',', ';', '\t', '|', ' ', '.', '_']

        if file_name.endswith('.csv') or file_name.endswith('.txt'):
            file_str = file_content.decode(errors='ignore')
            delimiter = next((delim for delim in possible_delimiters if delim in file_str), ',')
            df = pd.read_csv(StringIO(file_str), delimiter=delimiter, dtype=str)  # Keep everything as string initially
            logging.info(f"CSV/TXT file read successfully with delimiter '{delimiter}'.")

        elif file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(BytesIO(file_content), engine='openpyxl', dtype=str)  # Keep everything as string
            logging.info(f"Excel file read successfully: {file_name}.")

        else:
            logging.info(f"Unsupported file type {file_name}. Converting to CSV.")
            file_str = convert_to_csv(BytesIO(file_content), file_name)  # Function to convert other formats to CSV
            df = pd.read_csv(StringIO(file_str), delimiter=',', dtype=str)

        logging.info(f"Original columns: {df.columns}")
        df.columns = df.columns.str.strip()
        df.columns = df.columns.to_series().apply(lambda x: re.sub(r'\W+', '', x))
        logging.info(f"Cleaned columns: {df.columns}")

        # Get the specific mappings for the bank and transaction type
        mappings = BANK_MAPPINGS.get(bank_name, {}).get(transaction_type)

        if not mappings:
            raise ValueError(f"No mapping found for bank: {bank_name}, transaction type: {transaction_type}")

        cleaned_mapping_columns = [re.sub(r'\W+', '', col.strip()) for col in mappings['columns']]
        logging.info(f"Cleaned mapping columns: {cleaned_mapping_columns}")
        mappings['columns'] = cleaned_mapping_columns

        # Check for missing columns
        required_columns = mappings['columns']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing columns in DataFrame: {missing_columns}")
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

        # Filter columns based on the mapping
        df = df[required_columns]

        # Booking or refund-specific logic
        if transaction_type == 'booking':
            df['txn_date'] = df['txn_date'].apply(try_parse_date)
            df['credited_date'] = df['credited_date'].apply(try_parse_date)

            if df['txn_date'].isnull().any() or df['credited_date'].isnull().any():
                invalid_dates = df[df['txn_date'].isnull() | df['credited_date'].isnull()]
                logging.error(f"Invalid date formats found in booking data: {invalid_dates[['txn_date', 'credited_date']]}")

            bank_code = BANK_CODE_MAPPING.get(bank_name)
            if not bank_code:
                raise ValueError(f"No bank code found for bank: {bank_name}")

            for _, row in df.iterrows():
                row['irctc_order_no'] = int(row['irctc_order_no']) if pd.notnull(row['irctc_order_no']) else 0
                row['bank_booking_ref_no'] = int(row['bank_booking_ref_no']) if pd.notnull(row['bank_booking_ref_no']) else 0
                row['booking_amount'] = pd.to_numeric(row['booking_amount'], errors='coerce')

                if BookingData.objects.filter(irctc_order_no=row['irctc_order_no'], bank_booking_ref_no=row['bank_booking_ref_no']).exists():
                    logging.info(f"Duplicate booking found for IRCTC ORDER NO: {row['irctc_order_no']}. Skipping...")
                else:
                    BookingData.objects.create(
                        bank_code=bank_code,
                        txn_date=row['txn_date'],
                        credited_date=row['credited_date'],
                        booking_amount=row['booking_amount'],
                        irctc_order_no=row['irctc_order_no'],
                        bank_booking_ref_no=row['bank_booking_ref_no']
                    )
                    logging.info(f"Booking data saved for IRCTC ORDER NO: {row['irctc_order_no']}.")

        elif transaction_type == 'refund':
            df['refund_date'] = df['refund_date'].apply(try_parse_date)
            df['debited_date'] = df['debited_date'].apply(try_parse_date)

            if df['refund_date'].isnull().any() or df['debited_date'].isnull().any():
                invalid_dates = df[df['refund_date'].isnull() | df['debited_date'].isnull()]
                logging.error(f"Invalid date formats found in refund data: {invalid_dates[['refund_date', 'debited_date']]}")

            bank_code = BANK_CODE_MAPPING.get(bank_name)
            if not bank_code:
                raise ValueError(f"No bank code found for bank: {bank_name}")

            for _, row in df.iterrows():
                row['irctc_order_no'] = int(row['irctc_order_no']) if pd.notnull(row['irctc_order_no']) else 0
                row['bank_booking_ref_no'] = int(row['bank_booking_ref_no']) if pd.notnull(row['bank_booking_ref_no']) else 0
                row['bank_refund_ref_no'] = int(row['bank_refund_ref_no']) if pd.notnull(row['bank_refund_ref_no']) else 0
                row['refund_amount'] = pd.to_numeric(row['refund_amount'], errors='coerce')

                if RefundData.objects.filter(irctc_order_no=row['irctc_order_no'], bank_booking_ref_no=row['bank_booking_ref_no']).exists():
                    logging.info(f"Duplicate refund found for IRCTC ORDER NO: {row['irctc_order_no']}. Skipping...")
                else:
                    RefundData.objects.create(
                        bank_code=bank_code,
                        refund_date=row['refund_date'],
                        debited_date=row['debited_date'],
                        refund_amount=row['refund_amount'],
                        irctc_order_no=row['irctc_order_no'],
                        bank_booking_ref_no=row['bank_booking_ref_no'],
                        bank_refund_ref_no=row['bank_refund_ref_no']
                    )
                    logging.info(f"Refund data saved for IRCTC ORDER NO: {row['irctc_order_no']}.")

        logging.info(f"Finished processing file: {file_name}")
        return f"Successfully processed {file_name}"

    except Exception as e:
        logging.error(f"Error while processing file {file_name}: {str(e)}")
        return f"Failed to process {file_name}: {str(e)}"

# # Function to compare development and production DB data
# def compare_db_data(bank_name, year, month):
#     unmatched_records = []
#     # Fetch development data
#     development_data = BookingData.objects.filter(bank_name=bank_name, year=year, month=month)

#     # Fetch production data from production Oracle DB
#     with cx_Oracle.connect('user/password@production_db') as prod_conn:
#         prod_cursor = prod_conn.cursor()
#         prod_cursor.execute("SELECT * FROM production_booking_data WHERE bank_code=:bank_code", {'bank_code': BANK_CODE_MAPPING[bank_name]})
#         production_data = prod_cursor.fetchall()

#         # Compare and find unmatched records
#         for prod_row in production_data:
#             if not development_data.filter(irctc_order_no=prod_row[0]).exists():
#                 unmatched_records.append(prod_row)

#     return unmatched_records

# Function to convert non-CSV/Excel files to CSV
def convert_to_csv(file_content, file_name):
    # Implement conversion logic based on file type
    try:
        file_content.seek(0)  # Reset file pointer to the start
        content = file_content.read()

        # Handle ODS (OpenDocument Spreadsheet) files
        if file_name.endswith('.ods'):
            logging.info("Converting ODS file to CSV.")
            data = ods_get_data(file_content)
            # Assuming the first sheet contains the data you need
            sheet_data = data[next(iter(data))]
            df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])  # First row as header
            return df.to_csv(index=False)

        # Handle JSON files
        elif file_name.endswith('.json'):
            logging.info("Converting JSON file to CSV.")
            json_data = json.loads(content)
            df = pd.json_normalize(json_data)  # Flatten JSON if needed
            return df.to_csv(index=False)

        else:
            raise ValueError(f"Unsupported file format: {file_name}")

    except Exception as e:
        logging.error(f"Error converting file to CSV: {e}")
        return ""
