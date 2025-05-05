import pandas as pd
import numpy as np
import os
import re
import unicodedata
from email.utils import parseaddr
from datetime import datetime

# COLUMN_MAPPING defines standardized column names
COLUMN_MAPPING = {
    'from': 'Sender',
    'to': 'Recipient',
    'date': 'Date',
    'subject': 'Subject',
    'body': 'Body'
}

# OUTPUT_COLUMNS defines the output column order
OUTPUT_COLUMNS = ['Sender', 'Recipient', 'SenderDomain', 'Date', 'Subject', 'Body']

class EmailDataProcessor:
    def __init__(self):
        self.stats = {
            'initial_rows': 0,
            'junk_rows': 0,
            'null_value_counts': {},
            'output_columns': OUTPUT_COLUMNS
        }
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    def load_file(self, file_path):
        """Load CSV or Excel file with error handling for encoding."""
        try:
            if file_path.endswith('.csv'):
                encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
                for encoding in encodings:
                    try:
                        data = pd.read_csv(file_path, encoding=encoding)
                        print(f"Loaded file with encoding: {encoding}")
                        break
                    except Exception:
                        continue
            elif file_path.endswith(('.xls', '.xlsx')):
                data = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel.")
            self.stats['initial_rows'] = len(data)
            return data
        except Exception as e:
            raise ValueError(f"Error loading file: {e}")

    def extract_email_components(self, data):
        """Extract sender name, recipient name, and domain from email addresses."""
        data['Sender'] = data['Sender'].apply(lambda x: parseaddr(x)[1] if pd.notnull(x) else 'NULL')
        data['Recipient'] = data['Recipient'].apply(lambda x: parseaddr(x)[1] if pd.notnull(x) else 'NULL')
        data['SenderDomain'] = data['Sender'].apply(lambda x: self.extract_domain(x))
        return data

    def extract_domain(self, email):
        """Extract domain from email address."""
        if email and '@' in email:
            return email.split('@')[-1]
        return 'NULL'

    def parse_email_date(self, data):
        """Normalize date to a standard format."""
        def normalize_date(date):
            try:
                return pd.to_datetime(date).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return 'NULL'
        data['Date'] = data['Date'].apply(normalize_date)
        return data

    def clean_text(self, text):
        """Remove invalid characters and normalize Unicode."""
        if pd.isnull(text):
            return 'NULL'
        text = unicodedata.normalize('NFKC', str(text))
        text = re.sub(r'[^\x00-\x7F]+', '', text)  # Remove non-ASCII characters
        return text.strip()

    def is_junk_text(self, text):
        """Detect meaningless junk data."""
        if pd.isnull(text) or text == 'NULL':
            return True
        if re.match(r'^[A-Za-z]{4,}[fA-Z?]+$', text):  # Junk patterns like "AAAfAAAAA"
            return True
        if len(set(text)) == 1:  # Repeated characters
            return True
        return False

    def is_junk_row(self, row):
        """Identify rows with too many NULL values or junk patterns."""
        null_values = row.isnull().sum()
        if null_values > len(row) / 2:
            return True
        if any(self.is_junk_text(str(row[col])) for col in ['Sender', 'Subject', 'Body']):
            return True
        return False

    def process_data(self, data):
        """Clean and standardize email data."""
        # Map columns
        data.rename(columns=COLUMN_MAPPING, inplace=True)
        missing_cols = [col for col in COLUMN_MAPPING.values() if col not in data.columns]
        for col in missing_cols:
            data[col] = 'NULL'
        # Extract email components
        data = self.extract_email_components(data)
        # Normalize dates
        data = self.parse_email_date(data)
        # Clean text
        for col in ['Sender', 'Recipient', 'Subject', 'Body']:
            data[col] = data[col].apply(self.clean_text)
        # Truncate body content
        data['Body'] = data['Body'].apply(lambda x: x[:1000] if x != 'NULL' else x)
        # Remove junk rows
        data['is_junk'] = data.apply(self.is_junk_row, axis=1)
        self.stats['junk_rows'] = data['is_junk'].sum()
        data = data[~data['is_junk']]
        # Calculate NULL value stats
        self.stats['null_value_counts'] = data.isnull().sum().to_dict()
        self.stats['null_value_percentages'] = (data.isnull().mean() * 100).to_dict()
        return data[OUTPUT_COLUMNS]

    def generate_report(self, data):
        """Generate a text report with processing statistics."""
        report = [
            f"Processing Report ({self.timestamp})",
            "=" * 50,
            f"Total rows initially loaded: {self.stats['initial_rows']}",
            f"Rows removed as junk: {self.stats['junk_rows']}",
            "NULL value counts and percentages:",
            "\n".join([f"  {col}: {count} ({pct:.2f}%)"
                        for col, (count, pct) in zip(self.stats['null_value_counts'].keys(),
                                                     zip(self.stats['null_value_counts'].values(),
                                                         self.stats['null_value_percentages'].values()))]),
            f"Output Columns: {', '.join(OUTPUT_COLUMNS)}",
            "Sample Processed Data (First 5 Rows):",
            str(data.head().to_string(index=False))
        ]
        return "\n".join(report)

    def save_report(self, data, report):
        """Save processed data and report with timestamped filenames."""
        base_name = f"email_data_{self.timestamp}"
        data_file = f"{base_name}.csv"
        report_file = f"{base_name}_report.txt"
        data.to_csv(data_file, index=False)
        with open(report_file, 'w') as f:
            f.write(report)
        print(f"Processed data saved to: {data_file}")
        print(f"Report saved to: {report_file}")

def main():
    print("Welcome to Email Classifier!")
    file_path = input("Enter the path to the email data file (CSV or Excel): ").strip()
    processor = EmailDataProcessor()
    try:
        data = processor.load_file(file_path)
        processed_data = processor.process_data(data)
        report = processor.generate_report(processed_data)
        print(report)
        confirm = input("Do you want to save the processed files? (yes/no): ").strip().lower()
        if confirm == 'yes':
            processor.save_report(processed_data, report)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()