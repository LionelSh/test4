from typing import Sequence

# Import BaseOperator to create a custom operator by sublassing it
from airflow.models.baseoperator import BaseOperator 
from  airflow.hooks.postgres_hook import PostgresHook   # to interact with postgres SQL database
import logging
import pandas as pd

# create a custom operator by subclassing BaseOperator
class MyPostgresOperator(BaseOperator):
    template_fields: Sequence[str] = ("table", "postgres_conn_id")

    def __init__(
            self,            
            table,
            postgres_conn_id = "postgres_conn_lime",
            **kwargs
        ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id: str = postgres_conn_id       # intializing the connection and table values 
        self.table: str = table
        logging.info("")

    def execute(self, context):
        # this is the absolute path of the transfromed cvs data
        logging.info("reading the csv file")

        filename ="./data/lime_online/Lime_online.csv"

        # import and read by pandas
        df_file = pd.read_csv(filename)

        # create an instance of the connection to the posgresdb container using the Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        logging.info("Connecting to postgres")

        # sql_alchemy_engine is a part of thepostgres hook
        # provides high level interface to interact with the database
        # instead of using pure sql statements
        engine = postgres_hook.get_sqlalchemy_engine()
        
        
        # use the engine with Pandas `to_sql` to write large amount of data to the database
        # instead of adding line by line
        df_file.to_sql(self.table, engine, if_exists="append", index=False)
        logging.info("inserting")

        