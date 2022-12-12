# Project: Data Pipelines
This is the fifth project of the Data Engineering Nanodegree from Udacity. 

It considers an imaginary startup called Sparkify which wants to analyze the data of its music streaming app to understand what songs their users are listening to. Initially, the data is only saved within `json` files and the startup wants a better way to query it.

The data is originated from files within two s3 buckets:
- **song play logs** - `s3://udacity-dend/log_data` (`JSON` format specified within `s3://udacity-dend/log_json_path.json`)
- **song data** - `s3://udacity-dend/song_data`

To do so, in this project, we use airflow to orchestrate our pipeline to transform and check the data. The transformed data is saved in following tables:
- `songplays` (fact table) - log of song plays
- `users` (dimension table) - users in the app
- `songs` (dimension table) - songs the app as in its database
- `artists` (dimension table) - data about the song artists in the database
- `time` (dimension table) - timestamps of the start time for every record in the songplays table broken down in specific units

The final tables are in the star schema, making it easy to aggregate data on the songplays fact table and at the same time easy to join with dimension tables for filtering and specify aggregation parameters.

The airflow dag executes following steps:

![airflow-dag](/img/dag.png)
## How to run the code
1. Create IAM user with:
    1. Programatig access
    2. Attached Policies:
        - AdministratorAccess
        - AmazonRedshiftFullAccess
        - AmazonS3FullAccess
2. Create Redshift Cluster and:
    1. Make it publicly accessible (Actions > Modify publicly accessile setting)
    2. Enable `Enhanced VPC Routing`
3. Run `docker compose up -d` and log in to Airflow on `localhost:8080`
    - user: `admin`
    - password: `admin`
4. Add connections with above created credentials and cluster inf to Airflow:
    - Amazon Web Services: name `aws_credentials`
    - Postgres: name `redshift`
5. Enable the dag
## Main files
The project consists of following files:
- `docker-compose.yml` - Creates the docker containers, volumes and network
- `plugins/operators/*.py` - Custom operators used by the dag
- `plugins/helpers/sql_queries.py` - SQL queries to transform the data. Used within the dag.
- `dags/sparkify_etl.py` - Airflow dag
- `pyproject.yml` and `poetry.lock` - Files for the virtual environment (used for the development)
## References
The project is based on the initial project files and used the guidance provided by the Udacity Data Engineering Nanodegree.

The `docker-compose.yml` is based on Bitnami's instructions and file, which can be found [here](https://github.com/bitnami/bitnami-docker-airflow)