# Creating a pipeline using Airflow

## Description

This project is a hands-on workshop on how to build an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The main goal is to demonstrate how to extract information from two different data sources (CSV file, database), perform data transformations, merge the transformed data, and finally load it into Google Drive as a CSV file and store it in a database. As a final step, we will create a dashboard from the data stored in the database to visualize the information in the best way possible.

## Prerequisites

Before getting started with this project, make sure you have the following components installed or ready:

- [Apache Airflow](https://airflow.apache.org/)
- [Python](https://www.python.org/)
- [Google Drive API](https://developers.google.com/drive)
- [Database (can be local or cloud-based, if it's local I recommend using MySQL)](https://www.mysql.com/)

## Environment Setup

Here are the steps to set up your development environment:

1. **create a virtual enviroment**: Run the following command to create a virtual enviroment called venv:

   ```bash
   python -m venv venv

2. **activate your venv**: Run the following commands to activate the enviroment:

   ```bash
   cd venv/bin
   source activate

3. **Install Dependencies**: Once you're in the venv run the following command to install the necessary dependencies:

   ```bash
   pip install -r requirements.txt

4. **Create db_config**: Yo need to create a json file called "db_config" with the following information, make sure you replace the values with the correspondent information :

   ```bash
   {
    "user" : "myuser",
    "passwd" : "mypass",
    "server" : "XXX.XX.XX.XX",
    "database" : "demo_db"
   }  

5. **Airflow Scheduler**: Now go to the main folder and run the commands below to config airflow :

   ```bash
   airflow scheduler
   airflow standalone

6. **Running Server**: At this point airflow is running and we can run the etl_dag, in case you want to use the airflow interface the server is running in port 8080, credentials are shown in your terminal when you did airflow standalone, in case you want to run the dag in terminal run the code below:

   ```bash
   airflow trigger_dag etl_dag


## Contact

If you have any questions or suggestions, feel free to contact me at [lapiceroazul@proton.me].
