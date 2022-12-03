# Project: Data Pipelines
This is the fifth project of the Data Engineering Nanodegree from Udacity. 

Like in the other projects, it considers an imaginary startup called Sparkify which wants to analyze the data of its music streaming app to understand what songs their users are listening to. Unfortunately the startup has this data only within `json` files and wants a better way to query it.

The data is originated from files within two s3 buckets:
- **song play logs** - `s3://udacity-dend/log_data` with the JSON format specified within `s3://udacity-dend/log_json_path.json`
- **song data** - `s3://udacity-dend/song_data`

To do so, in this project, we use airflow to orchestrate our pipeline to transform and check the data. The transformed is saved in following tables:
- `songplays` (fact table) - log of song plays - Partitioned by: `year`, `month`
- `users` (dimension table) - users in the app
- `songs` (dimension table) - songs the app as in its database - Partitioned by: `year`, `artist_id`
- `artists` (dimension table) - data about the song artists in the database
- `time` (dimension table) - timestamps of the start time for every record in the songplays table broken down in specific units - Partitioned by: `year`, `month`

The final tables are in the star schema, making it easy to aggregate data on the songplays fact table and at the same time easy to join with dimension tables for filtering and specify aggregation parameters.
## How to run the code
[TODO]
## Files
The project consists of following files:
[TODO]
## Dependencies
All required dependencies are saved in the `pyproject.toml` / `poetry.lock` files. To install them create a virtual environment with Poetry ([link](https://python-poetry.org/))
## References
The project is based on the initial project files and used the guidance provided by the Udacity Data Engineering Nanodegree.
[TODO: Docker compose]