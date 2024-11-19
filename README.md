# ETL Data Pipeline with Airflow

![image](https://github.com/user-attachments/assets/e4106fea-d430-49a6-8de4-03c4ce3f3ecd)

This project is designed to build an ETL data pipeline specifically for Brazilian e-commerce data, leveraging Apache Airflow for orchestration. The pipeline automates the following steps:

1. Extracting raw data from MinIO, an object storage service.
2. Modeling and transforming data.
3. Loading the processed data into a PostgreSQL database.
4. Serving the data using Grafana for data visualization and business intelligence purposes.


## Deployment

 To run this project, you need to create a virtual environment and install neccesary libraries.

``` bash
  python3 -m venv venv

  source venv/bin/activate

  pip install -r requirements
```
Set AIRFLOW_HOME to current directory:
``` bash
  export AIRFLOW_HOME=$(pwd)
```
Initialize Airflow project:
``` bash
  airflow db init
``` 
Create admin users:
``` bash
  airflow users create \
         --username ... \
         --firstname  ... \
         --lastname ... \
         --role ... \
         --email ... 

```
Start AIRFLOW webserver:
``` bash
  airflow webserver -p 3030
```
Start AIRFLOW scheduler:
``` bash
  airflow scheduler
```
Start MinIO, Grafana and PostgreSQL containers
``` bash
  docker compose up -d
```
Go to [http://localhost:9000](http://localhost:9000) create MinIO bucket and upload data to.



## Demo
**DAG:** 

![image](https://github.com/user-attachments/assets/e5775b5f-089a-4418-84ac-485e89aa7bb6)

**Serving:**

![image](https://github.com/user-attachments/assets/d406495d-2958-4ea3-ae42-1f95ef9c3173)



## Tech Stack

**Data Processing:** Python

**Database and Data Storage:** PostgreSQL, MinIO

**Ochestration:** Airflow

**Visualization:** Grafana

**Containerization:** Docker

