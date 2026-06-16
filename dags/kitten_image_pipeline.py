import json
import requests
import requests.exceptions as request_exceptions
from pathlib import Path
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# ---------------------------------------------------------
# 1. DAG Instantiation
# ---------------------------------------------------------
dag = DAG(
    dag_id='Basic_Airflow_Pipeline',
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval=timedelta(days=1),
    catchup=False # Prevents Airflow from running past missed intervals
)

# ---------------------------------------------------------
# 2. Define Python Task Function
# ---------------------------------------------------------
def _get_kitten_images(json_file: str, save_path: str):
    """Parses the JSON file and downloads the images."""
    with open(json_file, 'r') as jsn_file:
        api_data = json.load(jsn_file)
        
    img_data_array = []
    for item in api_data:
        try:
            # Extract the 'small' image URL and download the content
            img_data_array.append(requests.get(item["urls"]["small"]).content)
        except request_exceptions.RequestException as e:
            print(f"Failed to download image: {e}")
            continue
            
    # Ensure the target directory exists
    save_path = Path(save_path)
    save_path.mkdir(parents=True, exist_ok=True)
    
    # Save the images using their Unsplash ID as the filename
    for i, item in enumerate(api_data):
        img_path = save_path.joinpath(f"{item['id']}.jpg")
        with open(img_path, 'wb') as img_file:
            img_file.write(img_data_array[i])

# ---------------------------------------------------------
# 3. Define the Tasks
# ---------------------------------------------------------

# IMPORTANT: Unsplash Developer API Key
CLIENT_ID = '<YOUR_UNSPLASH_ACCESS_KEY>' 

# Task 1: Fetch the data via Bash
fetch_kitten_urls = BashOperator(
    task_id="fetch_kitten_urls",
    bash_command=f"curl -o /opt/airflow/dags/images_urls.json -L --request GET 'https://api.unsplash.com/photos/random?query=kitten&count=2&client_id={CLIENT_ID}'",
    dag=dag
)

# Task 2: Process the data via Python
get_kitten_images = PythonOperator(
    task_id="get_kitten_images",
    python_callable=_get_kitten_images,
    op_kwargs={
        "json_file": "/opt/airflow/dags/images_urls.json",
        "save_path": "/opt/airflow/dags/fetched_images/"
    },
    dag=dag
)

# Task 3: Notify via Bash
pipeline_notification = BashOperator(
    task_id="pipeline_notification",
    bash_command='echo "There are now $(ls /opt/airflow/dags/fetched_images/ | wc -l) kitten images."',
    dag=dag
)

# NEW TASK: Write the results to a file instead of stdout
write_results_to_file = BashOperator(
    task_id="write_results_to_file",
    # We use the standard Linux redirect operator '>' to send the echo output to a text file
    bash_command='echo "There are now $(ls /opt/airflow/dags/fetched_images/ | wc -l) kitten images." > /opt/airflow/dags/kitten_state.txt',
    dag=dag
)

# ---------------------------------------------------------
# 4. Set Task Dependencies
# ---------------------------------------------------------

# By placing the last two tasks inside a list [], Airflow knows to run them in parallel!
fetch_kitten_urls >> get_kitten_images >> [pipeline_notification, write_results_to_file]