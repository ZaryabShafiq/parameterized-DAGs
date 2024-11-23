from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

# Function: Extract and Filter Data
cust = None
sales = None

def extract_and_filter_data(country=None, sales_channel=None):
    # Sample data loading logic
    global cust
    global sales
    cust = pd.read_csv('/path/to/customer_data.csv')
    sales = pd.read_csv('/path/to/sales_data.csv')

    # Apply filtering based on the passed parameter
    if country:
        cust = cust[cust['country'] == country]
    if sales_channel:
        sales = sales[sales['sales_channel'] == sales_channel]

    # Save filtered data for further use
    # cust.to_csv('/path/to/temp_filtered_customer.csv', index=False)
    # sales.to_csv('/path/to/temp_filtered_sales.csv', index=False)
    #
