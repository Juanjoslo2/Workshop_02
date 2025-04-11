# Libraries to work with Airflow
# --------------------------------

from datetime import datetime, timedelta
from airflow.decorators import dag, task


from tasks.etl import *

default_args = {
    'owner': "airflow",
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email': "example@example.com",
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    description='Creating an ETL pipeline.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    concurrency=4,
)

def workshop_2_dag():
    """
    This DAG is going to execute the ETL pipeline for the Global Terrorism Analysis project.
    
    """
    
    @task
    def api_extraction():
        return extract_api()
    
    api_raw_data = api_extraction()
    
    @task 
    def spotify_extraction():
        return extract_spotify()
    
    spotify_raw_data = spotify_extraction()
        
    @task
    def grammys_extraction():
        return extract_grammys()
    
    grammys_raw_data = grammys_extraction()
    
    @task
    def api_transformation(raw_df):
        return transform_api(raw_df)
    
    api_data = api_transformation(api_raw_data)
    
    @task
    def spotify_transformation(raw_df):
        return transform_spotify(raw_df)
    
    spotify_data = spotify_transformation(spotify_raw_data)
    
    @task
    def grammys_transformation(raw_df):
        return transform_grammys(raw_df)
    
    grammys_data = grammys_transformation(grammys_raw_data)
    
    @task
    def data_merging(api_data, spotify_df, grammys_df):
        return merge_data(api_data, spotify_df, grammys_df)
    
    merged_data = data_merging(api_data, spotify_data, grammys_data)
    
    @task
    def data_loading(df):
        return load_data(df)
    
    data_load = data_loading(merged_data)
    
    @task
    def data_storing(df):
        return store_data(df)
    
    data_store = data_storing(data_load)
    
workshop_2_dag = workshop_2_dag()