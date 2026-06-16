# Airflow Kitten Image Pipeline

## Overview
This project is an automated, fault-tolerant data pipeline orchestrated with **Apache Airflow** and containerized using **Docker Compose**. It automates the process of querying the Unsplash API for kitten images, parsing the response, downloading the images to a local directory, and logging the pipeline's state.

This project was built as part of the ALX Data Engineering curriculum to demonstrate workflow orchestration, task dependencies, atomicity, and idempotency.

## Features
* **Automated API Ingestion:** Uses Airflow's `BashOperator` to fetch JSON metadata from the Unsplash API.
* **Data Processing:** Uses the `PythonOperator` to parse the JSON and extract/download the actual JPEG images.
* **Parallel Execution (Branching):** Executes parallel downstream tasks to simultaneously log a success notification to standard output and write the system state to a local text file.
* **Containerized Environment:** Fully isolated infrastructure using Docker.

## Tech Stack
* **Python 3**
* **Apache Airflow** (Workflow Orchestration)
* **Docker & Docker Compose** (Containerization)
* **Bash** (Shell scripting & curl)
* **Unsplash API** (Data Source)

## 📁 Project Structure
```text
airflow_docker_project/
├── dags/
│   ├── kitten_image_pipeline.py    # The main Airflow DAG script
│   ├── kitten_state.txt            # Auto-generated pipeline state log
│   ├── images_urls.json            # Auto-generated temporary JSON data
│   └── fetched_images/             # Auto-generated folder containing downloaded JPEGs
├── logs/                           # Airflow execution logs
├── plugins/                        # Airflow custom plugins (empty)
├── docker-compose.yaml             # Official Airflow Docker configuration
├── .env                            # Environment variables (UID)
└── .gitignore                      # Ignored files to protect secrets and data


```
## Setup and Installation

### Prerequisites

* Docker Desktop installed and running.
* An active [Unsplash Developer API Key](https://unsplash.com/developers).

### Instructions

1. **Clone the repository:**
```bash
git clone [https://github.com/YOUR_USERNAME/airflow-kitten-pipeline.git](https://github.com/YOUR_USERNAME/airflow-kitten-pipeline.git)
cd airflow-kitten-pipeline
```
2. **Add your API Key:**
Open `dags/kitten_image_pipeline.py` and replace `<YOUR_UNSPLASH_ACCESS_KEY>` with your actual Unsplash Client ID.
3. **Set your Airflow UID:**
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

4. **Initialize the Airflow Database:**
```bash
docker compose up airflow-init

```

5. **Start the Services:**
```bash
docker compose up -d

```

6. **Trigger the Pipeline:**
* Navigate to `http://localhost:8080` in your browser.
* Log in with username `airflow` and password `airflow`.
* Unpause and trigger the `Basic_Airflow_Pipeline` DAG.
* Check the local `dags/fetched_images/` directory to view the downloaded kittens!



## Teardown

To stop the containers and clean up the environment, run:

```bash
docker compose down

```


### **Step 3: Commit and Push to GitHub**
Now that your README is saved, we just need to tell Git to track this new file and push it up to your remote repository. Run these commands in your terminal:

```bash
# 1. Stage the new README file
git add README.md

# 2. Commit the change
git commit -m "docs: add comprehensive project README"

# 3. Push the update to GitHub
git push origin main

```

