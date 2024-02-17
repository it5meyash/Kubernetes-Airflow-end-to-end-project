from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd


def get_data(**kwargs):
    url = "https://github.com/it5meyash/Kubernetes-Airflow-end-to-end-project/raw/master/booking.csv"
    response = requests.get(url)

    if response.status_code == 200:  # Corrected the status_code check
        df = pd.read_csv(url, header=None, names=['client_id', 'booking_date', 'room_type', 'hotel_id', 'booking_cost', 'currency'])

        # Convert dataframe to json string from xcom
        json_data = df.to_json(orient='records')

        kwargs['ti'].xcom_push(key='data', value=json_data)

    else:
        raise Exception(f'Failed to get data, HTTP status code: {response.status_code}')


def preview_data(**kwargs):  # Corrected function name from priview to preview
    import pandas as pd
    import json

    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCom')

    # Create Dataframe from JSON data
    df = pd.DataFrame(output_data)

    # Compute total sales
    df['Total'] = df['booking_cost'] * df.groupby('hotel_id')['hotel_id'].transform('count')

    # Sort by booking_cost
    df = df.sort_values(by='Total', ascending=False)

    print(df[['room_type', 'Total']].head(20))


default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 1, 25),
    'catchup': False,
}

dag = DAG(
    'fetch_and_preview',
     default_args=default_args,
     schedule_interval=timedelta(days=1)  # Changed schedule to schedule_interval
)

get_data_from_url = PythonOperator(
     task_id='get_data',
     python_callable=get_data,
     dag=dag,
 )

preview_data_from_url = PythonOperator(
     task_id='preview_data',
     python_callable=preview_data,
     dag=dag,
 )

get_data_from_url >> preview_data_from_url

