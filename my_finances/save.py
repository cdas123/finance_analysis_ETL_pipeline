import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

load_dotenv()

def write_dataframe_to_snowflake(df, schema, if_exists='replace'):
    """
    Writes a Pandas DataFrame to a Snowflake table.

    Parameters:
    - df1 (pd.DataFrame): The DataFrame1 to upload to Snowflake.
    - df2 (pd.DataFrame): The DataFrame2 to upload to Snowflake.
    - if_exists (str): 'replace' to drop and recreate the table, 'append' to add data (default: 'replace').

    Returns:
    - str: Success message or error message.
    """

    # Step 1: Create a SQLAlchemy engine to connect to Snowflake
    engine = create_engine(URL(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    ))

    try:
        # Step 2: Write the DataFrame to Snowflake
        df.to_sql(table_name, con=engine, index=False, if_exists=if_exists, method='multi')
        return f"DataFrame successfully written to the Snowflake table '{table_name}'"
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        # Close the engine connection
        engine.dispose()
