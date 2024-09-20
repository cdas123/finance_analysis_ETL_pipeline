import os
import boto3
import snowflake.connector
import pandas as pd
from io import StringIO
from dotenv import load_dotenv


load_dotenv()

# Function to read CSV from S3
def read_csv_from_s3(filename: str):
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    csv_obj = s3.get_object(Bucket=os.getenv('S3_BUCKET_NAME'), Key=filename)
    body = csv_obj['Body'].read().decode('utf-8')
    print(pd.read_csv(StringIO(body),delimiter=';',skipinitialspace=True))
    return pd.read_csv(StringIO(body),delimiter=';',skipinitialspace=True)

# Function to load DataFrame to Snowflake
def load_dataframe_to_snowflake(df,table, schema):
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema,
        table=table
    )

    # Creating a cursor object
    cursor = conn.cursor()

    # Write the data to Snowflake
    success, nchunks, nrows, _ = cursor.write_pandas(df, table)

    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

    return success, nchunks, nrows

# Main function
def main():
    # Read CSV from S3
    df_ca = read_csv_from_s3('DE32100400000803506500_EUR_05-08-2024_1255.csv')
    df_visa = read_csv_from_s3('Visa_Premium-6550_05-08-2024_1254.csv')

    
    # Load DataFrame to Snowflake
    success, nchunks, nrows = load_dataframe_to_snowflake(
        df_ca,
        'savings_raw',
        'savings_csv'
    )

    success, nchunks, nrows = load_dataframe_to_snowflake(
        df_visa,
        'visa_raw',
        'visa_csv'
    )
    
    print(f'Success: {success}, Number of chunks: {nchunks}, Number of rows: {nrows}')

if __name__ == '__main__':
    main()

