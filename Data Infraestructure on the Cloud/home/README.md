# Project Data Warehouse
### Author: Camilo Montenegro


## Running the ETL Pipeline

Follow this instructions to run the app locally. You need to setup previously a Redshift cluster in your AWS account and setup the variables in the dwh.cfg file

### Project Structure
1. create_tables.py drops and creates the tables. You run this file to reset your tables before each time you run the ETL scripts. It's important to configure the connection in case the enviroment of execution change
2. etl.py reads and processes files from song_data and log_data and loads them into your tables in the Readshift cluster. 
3. sql_queries.py contains all the sql queries, and is imported into the last two above files.
4. README.md (this file) provides discussion about the project.
5. GraphsAnalytics.ipynb contains some example analytics queries perfomed on the database and its graphs
6. Images folder contains images about the project

### Prerequisites

```
Python > 3.x
AWS Account
Jupyter lab/notebook
```

### Local Deployment on terminal
Download all files
```
python create_tables.py
python etl.py
jupyter lab
open GraphsAnaytics.ipynb and run the notebook to see some graphs
```
### Deployment on Udacity Env.
```
open console terminal
run python create_tables.py
run python etl.py
open GraphsAnalytics.ipynb and run the notebook to see some graphs
```

## Description of the project


The startup Sparkify is interested in analyzing all the data gathered by their new music streaming app. This data contains important information of their users and how they are moving around the app, this in terms of which songs are they listening to, in which devices, in which geographic location and some other information. Here they have an important asset that the startup can explode to understand users' preferences, activity, favorite songs at the moment and with this, they can recommend some music, therefore, increasing user experience and the number of users using the streaming app. The main analytic goal for Sparkify could be to gather all the possible information and create a recommender system that can adjust to the distinct users' profiles.

## StarSchema Design
<img src="images/StarSchema.png" width="750" height="750">

The previous image presents the Star Schema defined for this Data Modeling process. In the middle of the graph, the fact table contains main foreigns keys to the other database tables, at the bottom, there're defined some measurements, metrics, and facts that can resolve some business process questions. The answer to this and other questions can be easily found thanks to the distributed and relational schema made. It is clear to see in this schema that the database is normalized so there're few or none dependencies, the tables contain foreign keys to other tables and in this way, the database is more informative.

## ETL Pipeline

The ETL Pipeline is very straighforward and easy to understand. The main goal of this script is to read all the semi-structured information found in the .JSON files located at a AWS S3 Bucket and load them in each database table. For this process is important to make some previous validations and transform the data so the Star Schema can be accomplish.
Initial, the pipeline create two staging tables with the purpose of loading all .JSON data and then extract only the needed columns to the starschema tables.

## Song Play Analytics

### Amount of Songs listened by user

|i     |Count|User Id|Name      |Gender|
|------|-----|-------|----------|------|
|0     |294  |80     |Tegan     |F     |
|1     |236  |49     |Chloe     |F     |
|2     |118  |15     |Lily      |F     |
|3     |112  |29     |Jacqueline|F     |
|4     |102  |88     |Mohammad  |M     |
|5     |86   |36     |Matthew   |M     |
|6     |84   |97     |Kate      |F     |
|7     |77   |24     |Layla     |F     |
|8     |62   |16     |Rylan     |M     |
|9     |59   |44     |Aleena    |F     |
|10    |53   |73     |Jacob     |M     |
|11    |48   |85     |Kinsley   |F     |
|12    |45   |95     |Sara      |F     |
|13    |29   |25     |Jayden    |M     |
|14    |28   |30     |Avery     |F     |
|15    |27   |10     |Sylvie    |F     |
|16    |23   |58     |Emily     |F     |
|17    |14   |26     |Ryan      |M     |
|18    |14   |50     |Ava       |F     |
|19    |14   |72     |Hayden    |F     |
|20    |13   |42     |Harper    |M     |
|21    |12   |82     |Avery     |F     |
|22    |12   |66     |Kevin     |M     |
|23    |7    |12     |Austin    |M     |
|24    |6    |32     |Lily      |F     |
|25    |6    |101    |Jayden    |M     |
|26    |5    |65     |Amiya     |F     |
|27    |5    |60     |Devin     |M     |
|28    |5    |52     |Theodore  |M     |
|29    |5    |92     |Ryann     |F     |
|30    |4    |86     |Aiden     |M     |
|31    |4    |70     |Jaleah    |F     |
|32    |4    |37     |Jordan    |F     |
|33    |4    |78     |Chloe     |F     |
|34    |3    |8      |Kaylee    |F     |
|35    |3    |63     |Ayla      |F     |
|36    |3    |53     |Celeste   |F     |
|37    |2    |91     |Jayden    |M     |
|38    |2    |69     |Anabelle  |F     |
|39    |2    |100    |Adler     |M     |
|40    |2    |14     |Theodore  |M     |
|41    |2    |43     |Jahiem    |M     |
|42    |2    |6      |Cecilia   |F     |
|43    |2    |54     |Kaleb     |M     |
|44    |2    |35     |Molly     |F     |
|45    |2    |33     |Bronson   |M     |
|46    |2    |76     |Jayden    |F     |
|47    |2    |81     |Sienna    |F     |
|48    |2    |83     |Stefany   |F     |
|49    |2    |67     |Colm      |M     |
|50    |2    |40     |Tucker    |M     |
|51    |2    |2      |Jizelle   |F     |
|52    |2    |23     |Morris    |M     |
|53    |2    |89     |Kynnedi   |F     |
|54    |1    |11     |Christian |F     |
|55    |1    |28     |Brantley  |M     |
|56    |1    |98     |Jordyn    |F     |
|57    |1    |62     |Connar    |M     |
|58    |1    |90     |Andrea    |F     |
|59    |1    |94     |Noah      |M     |
|60    |1    |34     |Evelin    |F     |
|61    |1    |13     |Ava       |F     |
|62    |1    |55     |Martin    |M     |
|63    |1    |57     |Katherine |F     |
|64    |1    |71     |Ayleen    |F     |
|65    |1    |75     |Joseph    |M     |
|66    |1    |61     |Samuel    |M     |


### Distribution of users genre
<img src="images/analytics/Genre.png" width="750" height="750">

### Top 5 Songs Name listened
<img src="images/analytics/Top 5 Songs Name listened.png" width="750" height="750">

### Top 5 Locations of music Listening
<img src="images/analytics/Top 5 Location.png" width="750" height="750">

### Top 5 of hours listening music 24h
<img src="images/analytics/Top 5 of hours listening music.png" width="750" height="750">

### Top 5 User Agent
<img src="images/analytics/Top 5 User Agent.png" width="750" height="750">

### Songs listened per month day
<img src="images/analytics/Song listened per month day.png" width="750" height="750">

### Songs listened per weekday
<img src="images/analytics/Song listened per weekday.png" width="750" height="750">