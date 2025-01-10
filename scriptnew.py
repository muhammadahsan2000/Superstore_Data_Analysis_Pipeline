import pandas as pd
import boto3
from datetime import datetime
import os
from io import StringIO

def process_and_upload_csv(
    input_csv_path: str,
    bucket_name: str,
    date_column: str,
    specific_date: str,  # Add this argument for the specific date
    s3_prefix: str = "order/snapshot_day="
) -> None:
    """
    Process CSV file and upload filtered data to S3 for a specific date.
    
    Args:
        input_csv_path (str): Path to input CSV file
        bucket_name (str): S3 bucket name
        date_column (str): Name of the date column in CSV
        specific_date (str): The date (e.g., '2017-01-02') to filter
        s3_prefix (str): Prefix for S3 folders
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Read CSV file
        print(f"Reading CSV file: {input_csv_path}")
        df = pd.read_csv(input_csv_path, encoding='ISO-8859-1')
        
        # Convert date column to datetime
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Convert specific_date to datetime format
        specific_date = pd.to_datetime(specific_date)
        
        # Filter data for the specific date
        filtered_data = df[df[date_column].dt.date == specific_date.date()]
        
        # Check if data exists for the specific date
        if filtered_data.empty:
            print(f"No data found for {specific_date.date()}.")
            return
        
        # Create partition folder name based on the specific date
        partition_folder = f"{s3_prefix}{specific_date.date()}"
        
        # Convert filtered DataFrame to CSV string
        csv_buffer = StringIO()
        filtered_data.to_csv(csv_buffer, index=False)
        
        # Define S3 key based on the specific date
        s3_key = f"{partition_folder}/orders_{specific_date.date()}.csv"
        
        # Upload to S3
        print(f"Uploading data for date {specific_date.date()} to S3...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Successfully uploaded to s3://{bucket_name}/{s3_key}")
            
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        raise

def main():
    # Configuration
    input_csv_path = "./datasets/samplesuperstore.csv"
    bucket_name = "s3-store-analysis-ah"
    date_column = "Order Date"
    specific_date = "2017-01-01"  # Set your specific date here
    
    # Process and upload
    process_and_upload_csv(
        input_csv_path=input_csv_path,
        bucket_name=bucket_name,
        date_column=date_column,
        specific_date=specific_date  # Pass the specific date here
    )

if __name__ == "__main__":
    main()