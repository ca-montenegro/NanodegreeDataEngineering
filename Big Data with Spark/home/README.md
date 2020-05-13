# Project Data Lake
### Author: Camilo Montenegro


## Running the ETL Pipeline

Follow this instructions to run the app locally. You need to setup previously a EMR cluster and S3 bucket in your AWS account and setup the variables in the dwh.cfg file

### Project Structure
1. etl.py reads and processes files from song_data and log_data and loads them into your tables in the S3 cluster using EMR. 
2. README.md (this file) provides discussion about the project.
3. etl.ipynb is a jupyter notebook used to develop the solution using a smaller data set

### Prerequisites

```
Python > 3.x
AWS Account
Jupyter lab/notebook
```

### Local Deployment on terminal
Download all files
```
python etl.py
```
### Deployment on Udacity Env.
```
run python etl.py
```

## Description of the project


The startup Sparkify is interested in analyzing all the data gathered by their new music streaming app. This data contains important information of their users and how they are moving around the app, this in terms of which songs are they listening to, in which devices, in which geographic location and some other information. Here they have an important asset that the startup can explode to understand users' preferences, activity, favorite songs at the moment and with this, they can recommend some music, therefore, increasing user experience and the number of users using the streaming app. The main analytic goal for Sparkify could be to gather all the possible information and create a recommender system that can adjust to the distinct users' profiles.

## StarSchema Design
<img src="images/StarSchema.png" width="750" height="750">

The previous image presents the Star Schema defined for this Data Modeling process. In the middle of the graph, the fact table contains main foreigns keys to the other database tables, at the bottom, there're defined some measurements, metrics, and facts that can resolve some business process questions. The answer to this and other questions can be easily found thanks to the distributed and relational schema made. It is clear to see in this schema that the database is normalized so there're few or none dependencies, the tables contain foreign keys to other tables and in this way, the database is more informative.

## ETL Pipeline

The ETL Pipeline is very straighforward and easy to understand. The main goal of this script is to read all the semi-structured information found in the .JSON files located at a AWS S3 Bucket and load them in each folder at a S3 bucket in parquet format, in this way the Data Lake is build. For this process is important to make some previous validations and transform the data so the Star Schema can be accomplish.
Initial, the pipeline create two staging tables with the purpose of loading all .JSON data and then extract only the needed columns to the starschema tables.
