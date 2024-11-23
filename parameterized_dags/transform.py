from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

# Function: Transform Data
def transform_data():
    # Load filtered data
    # cust = pd.read_csv('/path/to/temp_filtered_customer.csv')
    # sales = pd.read_csv('/path/to/temp_filtered_sales.csv')

    # Add your transformations here
    # CUSTOMER DATA
    global cust
    global sales
    curr_date = pd.to_datetime(datetime.today())
    cust['age'] = (curr_date - pd.to_datetime(cust['date_of_birth'])).dt.days // 365
    cust['ltd'] = (curr_date - pd.to_datetime(cust['created_at'])).dt.days // 365
    cust['full_name'] = cust['first_name'] + " " + cust['last_name']

    # SALES DATA
    sales_summary = sales.groupby('customer_id').agg(
        total_sales=('total_price', 'sum'),
        avg_sales=('total_price', 'mean')
    ).reset_index()

    # Merge transformed data
    cust = pd.merge(cust, sales_summary, on='customer_id', how='left')

    # Save the transformed data
    cust.to_csv('/path/to/transformed_customer_data.csv', index=False)

