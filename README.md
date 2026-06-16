# 🐾 Airflow Kitten Image Pipeline

## Overview
This project is an automated, fault-tolerant data pipeline orchestrated with **Apache Airflow** and containerized using **Docker Compose**. It automates the process of querying the Unsplash API for kitten images, parsing the response, downloading the images to a local directory, and logging the pipeline's state.

This project was built as part of the ALX Data Engineering curriculum to demonstrate workflow orchestration, task dependencies, atomicity, and idempotency.

## 🚀 Features
* **Automated API Ingestion:** Uses Airflow's `BashOperator` to fetch JSON metadata from the Unsplash API.
* **Data Processing:** Uses the `PythonOperator` to parse the JSON and extract/download the actual JPEG images.
* **Parallel Execution (Branching):** Executes parallel downstream tasks to simultaneously log a success notification to standard output and write the system state to a local text file.
* **Containerized Environment:** Fully isolated infrastructure using Docker.

## 🛠️ Tech Stack
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



