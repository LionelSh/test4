import logging             # a library for printing out remarks while running
import os                  # a library for saving files and reading files especially on local
                           # but was not used here up to this moment, "for future use"
import requests            # requests to deal with HTTP responses
import pandas as pd        # pandas is used to deal with data sets and manipulate the date easily and swiftly
from datetime import datetime
import json                # deal with JSON data formay


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook         
from airflow.models import Variable
from postgres_operator import MyPostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


##############################################################
#                                                            #
#       DAG ONLINE is dedicated for retrieving data          #
#   for each minute to illsutrate the sitaution on ground.   #
#   The data is saved locally and into postgres db           #
##############################################################


#  STEPS:
# (A) we need to import the data from API HTTP RESTFul interface
# (B) Manipulate the data transfer it from JSON to CSV
# (C) save the data as a csv for visualisation by tableau online
# (D) save to postgres to be avalable as history for trend analysis
# (E) save the same CSV file in S3 bucket AWS for backup for the enterprise
# (F) Declare the DAGs


# Parameters Settings

default_args = {
    "owner": "airflow",      
    "start_date": datetime(2023, 1, 1),
}

url_source='https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=1462'


####### (A) Import the data from interface #######

def _fetch_data_from_api(ti):

    logging.info("importing json data from url ")
    response = requests.get(url_source)         # using the HTTP GET method to retreive data from the target url
    data = response.json()                      # extract the JSON data available in HTTP Get request  


    filename = "imported_json_data.json"        # specifying a filename to save the json data

    #saving the json file in a local path of airflow, notice the path name
    logging.info("saving the json data")
    with open(f"./data/Lime_online/{filename}", "w") as f:
        json.dump(data, f)

    # Note: each time we import using HTTP request , the same JSON file will be replaced with the newer data

    logging.info(f"json data saved {filename} ")        # a message indicating save successfully
    ti.xcom_push(key="filename", value=filename)        # passing the filename to the subsequent process using xcom intercommunication 


###### (B) Manipulate the data and transfer it from JSON to CSV with the needed fields ######

def _transform_data(ti):
      # using xcom pulling the filename from the previous process
      filename = ti.xcom_pull(task_ids="fetch_data_from_api", key="filename")

      # reading the JSON file to parse and transfer the data
      f = open(f"./data/Lime_online/{filename}")        # note the data path
      data = json.load(f)                               # load the data as json format      
      f.close()                # must close after getting data to save resources , otherwise the application will pile up a file session each time
      
      # Define a pandas dataframe to contain all the needed data
      # define all the column names that we need 
      df = pd.DataFrame(columns=['name', 'stationcode', 'ebike', 'mechanical', 'x','y', 'duedate', 'numbikesavailable', 'numdocksavailable', 'capacity', 'is_renting', 'is_installed', 'nom_arrondissement_communes', 'is_returning','record_timestamp'])

      # Now we will extract the needed data available in JSON especially within the 'records' key
      # we will not use json normalize because there are data at different keys in different level in JSON structure
      

      for i in range (len(data['records'])):      # loop with the length of the number of sub-keys in the record key
        
        fields = data['records'][i]['fields']     # this is to avoid repetitionm , save typing and for code clarity
        
        processed_data = {
        "name": fields["name"],                    # the name of the station
        "stationcode" : fields['stationcode'],     # the code of the station
        'ebike' : fields['ebike'],                 # number of electrical bike available
        'mechanical':fields['mechanical'],         # number of mechanical bikes valable
        'y': fields['coordonnees_geo'][0],         # the x-coordinates longtitude of the station
        'x': fields['coordonnees_geo'][1],         # the y-coordinates latitude of the station 
        'duedate': fields['duedate'],
        'numbikesavailable': fields['numbikesavailable'], # number of electrical bike available
        'numdocksavailable': fields['numdocksavailable'], # number of mechanical bikes valable
        'capacity': fields['capacity'],                   # total capacity of bikes in the station 
        'is_renting': fields['is_renting'],        # how many are being rented
        'is_installed': fields['is_installed'],    # how many installed
        'nom_arrondissement_communes': fields['nom_arrondissement_communes'], 
        'is_returning': fields['is_returning'],              
        'record_timestamp': data['records'][i]['record_timestamp'],  # the time when this data was recorded at the source
        }

        # add this JSON processed data as a new line/row in the pandas data frame
        df.loc[i,:] = processed_data 

      # Finally after going through all the items save this data frame as a csv file
      # this csv file will be used by to transfer to postgres
      # and will be used by the online monitoring 
      
      filename = "./data/Lime_online/Lime_online.csv" 
      df.to_csv(filename,  index=False)
      logging.info("Data saved as csv")

      ti.xcom_push(key="filename", value=filename)   # pushing the file name for the upcoming process


      #### mission (C) is accomplished above : save the data as a csv for visualisation by tableau  ####

##### (D) save to postgres to be avalable as history and trend analysis  #####
##### (D) THIS STEP IS AVAILABLE WITHIN THE DAGS IT DIDN't WORK AS PYTHON OPERATOR  #####
###### IT WORKS AS A POSTGRES OPERATOR

#(D.1) Create the table if it does not exist
# smallint has been used to save storage space
# postgresOperator to support the execution of SQL commands for postgres




#### (E) upload the CSV file to AWS S3 bucket for backup   #### 

def _upload_to_s3_bucket():

    # using hook to create an instance of the aws connection
    s3_hook = S3Hook(aws_conn_id="aws_default")
    
    # the filename and path which we will get the data from in csv format
    filename = "./data/lime_online/Lime_online.csv"

    # setup the target filename which will be saved in S3 bucket
    # add in date and time in the filename
    now = datetime.now()
    file_to_save = f"Lime_online_{now}"

    # finally use the hook instance to upload the file into S# bucket AWS
    # filename: represents the name of the file which contains the data
    # key: represents the target file name that will be saved in AWS S3, we can set a path, but chose simple naming
    # the variable represents the name of the bucket where to store the data
    s3_hook.load_file(filename=filename, key=file_to_save, bucket_name=Variable.get("S3BucketName"))
    
    

##### (F) Declare the DAGs :) #######

#"dagrun_timeout":"timedelta(seconds=5)"
# finally declare the dag name and its processes
# Note the schedule_interval which indicates how frequent the dag processes should be launched
# here the process is launched every minute
# the expression '*/1 * * * *' is a crone table expression indicating each minute

with DAG("Lime-online-dag", default_args=default_args,schedule_interval= '*/1 * * * *',catchup=False) as dag:
    fetch_data_from_api = PythonOperator(task_id="fetch_data_from_api", python_callable=_fetch_data_from_api)
    transform_data = PythonOperator(task_id="transform_data", python_callable=_transform_data)
    
    # Dealing with postgress didn't work as python operator but as a PostgresOperator
    #create_postgres_table = PythonOperator(task_id="create_postgres_table" , python_callable=_create_postgres_table)
    
    create_postgres_table = PostgresOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS tbl_lime_resources (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            stationcode integer,
            ebike smallint,
            mechanical smallint,
            y decimal,
            x decimal,
            duedate timestamp,
            numbikesavailable smallint,
            numdocksavailable smallint,
            capacity smallint,
            is_renting VARCHAR,
            is_installed VARCHAR,
            nom_arrondissement_communes VARCHAR,
            is_returning VARCHAR,
            record_timestamp timestamp
            )
        """,
        postgres_conn_id="postgres_conn_lime",
    )
     

    

    insert_in_postgres_table = MyPostgresOperator(
        task_id="insert_in_table",
        table = "tbl_lime_resources",
        postgres_conn_id="postgres_conn_lime",
        
    )
    

    #insert_in_postgres_table = PythonOperator(task_id="insert_in_postgres_table", python_callable=_insert_in_postgres_table)
    upload_to_s3_bucket = PythonOperator(task_id="upload_to_s3_bucket", python_callable=_upload_to_s3_bucket)
    
    # processes order within the tag
    fetch_data_from_api >> transform_data >> create_postgres_table >> insert_in_postgres_table >> upload_to_s3_bucket  



