import pandas as pd
import snowflake.connector

# Load data
df = pd.read_csv('../data/sales_data.csv')

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='YOUR_DATABASE',
    schema='PUBLIC'
)
cursor = conn.cursor()

# Upload data
for index, row in df.iterrows():
    cursor.execute(
        f"""
        INSERT INTO SALES_DATA (DATE, REGION, PRODUCT, SALES)
        VALUES ('{row['date']}', '{row['region']}', '{row['product']}', {row['sales']})
        """
    )

cursor.close()
conn.close()
