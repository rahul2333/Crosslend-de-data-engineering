"""
 * Company: Crosslend
 * ----------------------------------------------------------------------------
 * SUMMARY:
 * NYC Taxi Project and job level configurable parameters will be stored here
 * ----------------------------------------------------------------------------
"""

############
# Built-in #
############

# Google Cloud Project ID
PROJECT = 'nyc-taxi-analytics'

# Region Name for all project related resources
REGION = 'us-east1'

# Composer gcs directory path
COMPOSER_GCS_PATH = '/home/airflow/gcs/'
# Service Account name for the account being used to run the tasks
SERVICE_ACC_NAME = 'nyc-taxi@nyc-taxi-analytics.iam.gserviceaccount.com'

# Bucket where json key file for nyc taxi analytics service account has been stored
KEY_BUCKET = 'us-east1-nyc-taxi-analytics-9583489a-bucket'
# Path and name of json key file for nyc-taxi-analytics service account
KEY_FILE = 'data/nyc-taxi-key-store/nyc-taxi-analytics-42dfc742260c.json'

# GCS storage details for nyc taxi analytics
NYC_TXI_BUCKET_NAME = 'nyc-taxi-analytics-staging'
NYC_TXI_BUCKET_LOC = 'gs://' + NYC_TXI_BUCKET_NAME + '/'
STAGING_LOC = 'staging'
PROCESSED_LOC = 'processed'

# Local machine folder path
Path = "C:\\Users\\shyam sunder\\PycharmProjects\\Crosslend\\"

# Cloud SQL PostgresSQL DB Instance Connection details
USERNAME = 'postgres'
PASSWORD = 'Pt8daJgsr7mJe0mA'
HOST = 'sqlproxyservice'
PORT = 5432
DB_NAME = 'nyc-taxi-analytics'
DB_SCHEMA = 'public'

# Cloud SQL MySQL DB Instance Connection details
Database_user = 'root'
Database_password = '123456'
Host = 'localhost'
PORT1 = 3306
Database_name = 'nyc_taxi'
DB_Schema = 'nyc_taxi'