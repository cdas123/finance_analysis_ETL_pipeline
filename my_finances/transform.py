import os
import boto3
import snowflake.connector
import pandas as pd
from io import StringIO
from dotenv import load_dotenv


load_dotenv()

# Function to load DataFrame to Snowflake
def fetch_dataframe_from_snowflake(df,table, schema):
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema,
        table=table
    )

    # Function to read raw data from a Snowflake table
def read_raw_data(conn, table_name):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, conn)

 # Example transformation function
def transform_data(df1, df2, on_column):
    
    # Normalize column names
    df1.columns = [col.strip().lower().replace(" ", "_") for col in df1.columns]
    
    # Fill missing values in a specific column (e.g., "buchungstext")
    df1['buchungstext'] = df1['buchungstext'].fillna(df1['buchungstext'].mean())
    
    # Group by 'bunchungstag' and calculate total purchase and average transaction value
    df_aggregated = df1.groupby('bunchungstag').agg({
        'transaction_amount': ['sum', 'mean'],
        'transaction_id': 'count'
    }).reset_index()

    # Rename the aggregated columns
    df_aggregated.columns = ['bunchungstag', 'total_purchase', 'average_transaction_value', 'transaction_count']

    # Merge the aggregated and pivot dataframes
    df_final = pd.merge(df_aggregated, df2, on='bunchungstag', how='inner')
    
    
    # Save the cleaned and transformed data for future use
    df_final.to_csv("cleaned_transformed_data.csv", index=False)
    print("Data cleaning and transformation completed.")
    
    return df_final
    

def merge_and_store(conn, table1, table2, target_table, on_column):
    # Step 1: Read data from both tables
    df1 = read_raw_data(conn, table1)
    df2 = read_raw_data(conn, table2)

    # Step 2: Merge the dataframes on the common column
    merged_df = transform_data(df1, df2, on_column)

    # Step 3: Write the merged data back to Snowflake
    write_to_snowflake(conn, merged_df, target_table)

     


