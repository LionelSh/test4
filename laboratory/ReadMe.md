Welcome to Jedha-final project entited Lime 

A) Objective
to manage the resources of the LIME enterprise

B) Before starting
please take into account the follwoing settings:
    1. Postgress Database connection
        a. establish a postgres database container using the postgres image
        hereby is the command:
        
        $ docker run --rm -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password --name lime-postgres -p 5433:5432 postgres

        Remark(1): note the name of the postgres container "lime-postgres"
        Remark(2): note the port mapping between the host and the container, we used 5433. why? 
                   because Airflow has its own progres db that uses the default postgres port "5432"
                   to prevent conflict between Airflow postgres and ours.

        b. configure the postgres connection within Airflow environment under admin
        hereby are the settings:

        Connection Id: postgres_conn_lime
        Connection Type: Postgres
        Host : host.docker.internal
        Login: user
        Password: password
        Port: 5433

        c. if you wish to check manually the data in the tables:
           c.1 enter the postgres docker:
             docker exec -it lime-postgres bash
           c.2 enter into the postgres db with root access 
             plsql -U user
           c.3 access the lime data base which is here named posgres
             \du postgres
           c.3 finally, show the data available in the table:
             select * from tbl_lime_resources 

    2. AWS S3 bucket connection:
       a. within Airflow -> admin -> connection, configure the AWS connection

        Connection Id: aws_default
        Connection Type: Amazon S3
        Extra: {"aws_access_key_id": "YOUR_ACCESS_KEY_ID", "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY"}

        b. within Airflow -> Admin -> Variables, add a variable to communicate with the s3 bucket

        Key: S3BucketName
        Val: limeproject
        Description: S3 bucket name

C) finally initialize the Airflow and compose it with build:
    $ docker-compose up airflow-init
    $ docker-compose up --build

    and ENJOY THE RIDE :)

       

