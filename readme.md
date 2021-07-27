# CrossLend [![Build Status](https://secure.travis-ci.org/qualiancy/breeze-dag.png?branch=master)](https://github.com/rahul2333/Crosslend-de-data-engineering)

> NYC Taxi Analytics Project

# Crosslend Data Engineering Task
CrossLend data engineering solution repository. The main intention of the task is to create a data pile i.e. ETL Job. Business analysts can query & explore this data later using SQL.
* Downloads the New York city Taxi trips data from New York City Taxi & Limousine Commission (NYC TLC)
* Sinks the data to MySQL database
* As per the tasks calcualtes the most popular destinations based on the criterias 
* Ingest only the changed tracked records to the database

## Data Pipe line Approach: 
Once the data has been downloaded to GCS bucket
It will ingest the data to MySQL database to their respective tables like 'nyc_taxi', 'nyc_taxi_lookup' tables
Orchestration is through composer/airflow

## Architecture Flow
![FlowDiagram](https://github.com/rahul2333/Crosslend-de-data-engineering/blob/main/Images/crosslend_png.png)


## Assumptions
- The solution is designed to deploy it on Google Cloud Platform
- All the services like Google Cloud Storage, Data flow, Cloud SQL should be spinned up   and running with all the required privilages to the service account
- Currently the solution is developed to execute it on local and as well as on GCP Cloud
- Currently the paths are harcoded to local machine and can be replaced with gcp buckets

### Tech Stack: 
            - Python(Pandas, SqlAlchemy),
            - MySQL
            - Airflow

## To set up the application on your machine
* Clone this repository to your local machine and create a python3 virtualenv environment, and later install the dependencies.
  * `virtualenv env python3`
  * `source env/bin/activate`
  * `pip install -r requirements`
* Update the config file with respect to project, paths and database configurations.
* As a prequistie database should be created as 'nyc_taxi'

## Task related answers
* The data for task 2-1 is computed and can be viewed from where rnk is the top-K 'nyc_taxi.nyc_taxi_pop_dest_pickups'

    ```sql
    SELECT * FROM nyc_taxi.nyc_taxi_pop_dest_pickups
    WHERE rnk=1;
    ```

* The data for task 2-2 is computed and can be viewed from where rnk is the top-K 'nyc_taxi.nyc_taxi_pop_dest_pickups'

    ```sql
    SELECT * FROM nyc_taxi.nyc_taxi_pop_boroughs
    Where PUBorough ='Manhattan';
    ```
* Task 3  historical load with only changes will be loaded to 'nyc_taxi_history' table
and can be queried as below
 
    ```sql
    SELECT * FROM nyc_taxi.nyc_taxi_history;
    ```
    
* Task 4  Only the current data can be viewed in 'nyc_taxi_curr' table
and can be queried as below
 
    ```sql
    SELECT * FROM nyc_taxi.nyc_taxi_curr;
    ```

* To add further information based on the underlying data we can calculate avgerage total amount, average trip amount from the below SQL query:
    ```sql
    SELECT 
        date_format(tpep_pickup_datetime, '%Y-%m') as mnth,
        ROUND(AVG(total_amount), 2) AS avg_total_amt,
        ROUND(AVG(fare_amount), 2) AS avg_fare_amt
    FROM
        nyc_taxi.nyc_taxi
    GROUP BY DATE_FORMAT(tpep_pickup_datetime, '%Y-%m');
    ```
    we can also include filters like tip_amount >1 
    ```sql
    SELECT 
    date_format(tpep_pickup_datetime, '%Y-%m') as mnth,
    ROUND(AVG(total_amount), 2) AS avg_total_amt,
    ROUND(AVG(fare_amount), 2) AS avg_fare_amt
    FROM
    nyc_taxi.nyc_taxi
    WHERE tip_amount>1
    GROUP BY DATE_FORMAT(tpep_pickup_datetime, '%Y-%m');
    ```
* Short distance and long distance trips records
    ```sql
    SELECT count(*) AS count FROM nyc_taxi.nyc_taxi WHERE trip_distance < 30
    ```
    ```sql
    SELECT count(*) AS count FROM nyc_taxi.nyc_taxi WHERE trip_distance >= 30
    ```
* In order to view the daily trends we need to perform the group by based on dates and similary we can view it for yearly just by performing the transformations as shown above.

## Further possible enhancements
* The spinning of GCP services can be automated
* Better ways to handle exceptions  
* Instead of Cloud SQL it can be replaced with BigQuery for faster analytics purpose
* Using Bigquery will imporove performance faster by using partitions


## License
&copy; Rahul V 


Author: Rahul V

Email: [Rahul V](rahul.vb@hotmail.com)
