# importing needed libraries
import pandas as pd
from datetime import datetime
import os
import boto3
import logging
from botocore.exceptions import ClientError
import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType , IntegerType , TimestampType , DateType

# read & set the AWS credentials  
import configparser
config = configparser.ConfigParser()
config.read('cp.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# read original data 
locations_data_df=pd.read_json('locations_data.json')
taxi_trips_df=pd.read_csv('green_taxi_trips.csv')

def clean_datasets(locations_data_df,taxi_trips_df):
    """ 
    a function that drop duplicates and null values in both the datasets ,
    rename some columns in taxi trips dataset ,
    change the type of pickup_time and dropoff_time from string to datetime 
    change some columns types to match the schema types

    """
    locations_data_df = locations_data_df.dropna()  # drop null values in locations data
    
    locations_data_df = locations_data_df.astype({'LocationID':'int32'})
    # show new locations DataFrame characteristics 
    print('\n clean_locations_data shape is : ' + str(locations_data_df.shape))
    print('\n clean_locations_data nulls is : \n ' + str(locations_data_df.isnull().any()))
    print('\n clean_locations_data duplicates is : ' + str(locations_data_df.duplicated().any()))
    
    # saving clean locations data into new file
    locations_data_df.to_json('locations.json',orient='records')
    
    taxi_trips_df = taxi_trips_df.drop_duplicates() # drop duplicates in trips data
    # dropping null values in trips data
    taxi_trips_df = taxi_trips_df.dropna(axis=1,how='all') # erasing ('ehail') columns as its completely empty
    taxi_trips_df = taxi_trips_df.dropna(axis=0,how='any') # erasing rows with null values
    taxi_trips_df = taxi_trips_df.rename(columns={'lpep_pickup_datetime':'pickup_time',
                                                  'lpep_dropoff_datetime':'dropoff_time',
                                                  'store_and_fwd_flag':'store_and_fwd',
                                                  'passenger_count':'passengers',
                                                  'trip_distance':'distance'
            })  # changing some columns names for the ease of use 
    
    taxi_trips_df['pickup_time']=pd.to_datetime(taxi_trips_df['pickup_time']) # convert column type to datetime
    taxi_trips_df['dropoff_time']=pd.to_datetime(taxi_trips_df['dropoff_time']) # convert column type to datetime
    taxi_trips_df= taxi_trips_df.astype({'VendorID':'int32','PULocationID':'int32',
                            'DOLocationID':'int32','RatecodeID':'int32','passengers':'int32',
                            'payment_type':'int32','trip_type':'int32'})#change float columns types to int
    
    # show new trips DataFrame characteristics 
    print('\n clean_trips_data shape is : ' + str(taxi_trips_df.shape))
    print('\n clean_trips_data nulls is : \n ' + str(taxi_trips_df.isnull().any()))
    print('\n clean_trips_data duplicates is : ' + str(taxi_trips_df.duplicated().any()))

    # saving clean trips data into new file
    taxi_trips_df.to_csv('trips.csv',index=False)

def upload_files_to_s3(Bucket):
    """
    a function that upload the new clean data files to s3 bucket
    param : bucket --> s3 bucket name  

    """
    files = ['locations.json','trips.csv']
    for file in files:
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(file, Bucket, file)

def create_spark_session():
    """
    creating the spark session
    """
    spark = (
        SparkSession
        .builder
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .appName('capstone')
        .getOrCreate())
    
    return spark

def process_locations_data(spark):
    """
    a function that load locations data from s3 bucket 
    set schema for locations data and creation of locations table
    
    """
    # setting schema for locations data
    locations_schema = StructType([        
        StructField("LocationID", IntegerType()),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),        
        StructField("service_zone", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())])
    # get filepath to locations data file
    locations_path ='s3a://green-taxi-project/locations.json'
    # reading locations data file
    global locations_df
    locations_df=spark.read.json(locations_path,schema=locations_schema)
    # extract columns to create locations table ( dimenssion table )
    global locations_table
    locations_table=locations_df.select(['LocationID','Borough','Zone','service_zone','latitude','longitude'])

def process_trips_data(spark):
    """
    a function  that read trips data from s3 bucket
    set schema for trips data 
    create admin_table  , payment_table and  trips_table
    
    """
    
    # setting schema for Trips data
    trips_schema=StructType([        
        StructField("VendorID", IntegerType()),
        StructField("pickup_time", StringType()),
        StructField("dropoff_time", StringType()),
        StructField("store_and_fwd", IntegerType()),
        StructField("RatecodeID", IntegerType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("passengers", IntegerType()),
        StructField("distance", DoubleType()),        
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),        
        StructField("total_amount", DoubleType()),
        StructField("payment_type", IntegerType()),
        StructField("trip_type", IntegerType()),
        StructField("congestion_surcharge", DoubleType())
    ])
   
    # get filepath to locations trips file
    trips_path ='s3a://green-taxi-project/trips.csv'
    
    # reading trips data file
    global trips_df
    trips_df = spark.read.option("multiline", "true").option('header',True).csv(trips_path)
    trips_df=trips_df.withColumn('TripID', F.monotonically_increasing_id()) # create trip_ID unique column
   
    # extracting columns to create Administrative_table ( dim table )
    global admin_table
    admin_table=trips_df.select(['TripID','VendorID','store_and_fwd','RatecodeID']).dropDuplicates()
    
    # extract columns to create payments table ( fact table)
    global payments_table
    payments_table=trips_df.select(['TripID','fare_amount','extra',
                                    'mta_tax','tip_amount','tolls_amount',
                                    'improvement_surcharge','total_amount',
                                    'payment_type','trip_type','congestion_surcharge']).dropDuplicates()
   

    # creating Temp view to use sql
    locations_df.createOrReplaceTempView('locations_view')
    trips_df.createOrReplaceTempView('trips_view')

    # extracting columns to create Trips table ( dim table )
    global trips_table
    trips_table=spark.sql("""
                          SELECT DISTINCT 
                             t.TripID         AS TripID,
                             t.pickup_time    AS pickup_time,
                             t.dropoff_time   AS dropoff_time,
                             l.Zone           AS pickup_location,
                             l_2.Zone         AS dropoff_location,
                             t.distance       AS distance  ,
                             t.passengers     AS passengers,
                             t.trip_type      AS trip_type 
                          FROM trips_view     AS  t  
                          JOIN locations_view AS  l  
                          ON   l.LocationID = t.PULocationID 
                          JOIN locations_view AS  l_2  
                          ON   l_2.LocationID = t.DOLocationID 
                          """)        

def check_mandatory_columns():
    
    """
    a function that checks for mandatory columns 
    to be existed in dataframes ( columns loaded successfully)
    """
    print('\n' + ('_'*40))
    print('\n checking mandatory columns exists : \n')
    trips_mandatory_columns=['TripID','pickup_time','dropoff_time','PULocationID','DOLocationID','fare_amount','total_amount']
    locations_mandatory_columns=['LocationID','Zone','longitude','latitude']

    for column in trips_mandatory_columns:
        if column in trips_df.columns :
            print(f"\n Column {column} exists : PASSED ")
        else:
            print(f"\n Column {column} does not xist : FAILED")
    for column in locations_mandatory_columns:
        if column in locations_df.columns :
            print(f"\n Data quality on Column {column} PASSED  : Column exists ")
        else:
            print(f"\n Data quality on Column {column} FAILED   : Column does not xist : ")
            
def check_unique_columns():
    
    """
    a function that checks for unique values in  columns 
    that must have only unique values
    """ 
    print('\n' + ('_'*40))
    print('\n checking for unique values  : \n ')

    trip_id_unique=trips_table.select('TripID').distinct().count()
    location_id_unique=locations_table.select('LocationID').distinct().count()
    if trip_id_unique == trips_table.select('TripID').count():
        print("\n Data quality check passed : tripID contains only unique values")
    else :
        print("\n Data quality check failed : tripID contains duplicated values")
    if location_id_unique == locations_table.select('LocationID').count():
        print("\n Data quality check passed : LocationID contains only unique values")
    else :
        print("\n Data quality check failed : LocationID contains duplicated values")
        

def check_null_values():
    
    """
    a function that checks for null values in dataframes
     """
    print('\n' + ('_'*40))
    print('\n checking for null values : \n ')

    cols=['TripID','pickup_time','dropoff_time','PULocationID','DOLocationID','fare_amount','total_amount']
    for col in cols:
        if trips_df.filter(trips_df[col].isNull()).count() > 0:
            print("\n Data quality check failed " + col + " has null values ")
        else:
            print(f"\n Data quality check on {col} passed with 0  null values")

def check_records():
    
    """
    a function that checks for records exists
    to ensure tables was extracted successfully
    """
    print('\n' + ('_'*40))
    print('\n checking for records in tables : \n ')

    tables=[payments_table,trips_table,locations_table,admin_table]
    tables_names=['payments_table','trips_table','locations_table','admin_table']
    for table , table_name in zip(tables , tables_names):
        rows_counts = table.count()
        if rows_counts == 0 :
            print(f"\n Data quality check failed. {table_name} contained 0 rows")
        else :
            print(F"\n Data quality on  {table_name} check passed with {rows_counts} records")
    
def sql_sample(spark):
    """
    a function that will Provide evidence that the ETL has processed the result in the final data model.
    a sql statement to be extracted  
    """
    print('\n ' + ('_'*40))
    print( '\n sql query sample \n ')
    
    payments_table.createOrReplaceTempView('payments_view')
    trips_table.createOrReplaceTempView('trips_view')
    sample_query = spark.sql( """
                         SELECT 
                             p.TripID,        
                             t.pickup_location,
                             t.dropoff_location,
                             t.distance,
                             p.total_amount
                         FROM payments_view p 
                         JOIN trips_view t 
                         ON p.TripID = t.TripID 
                         WHERE t.distance > 10.0 
                         limit 10
                         """).show()
    
def main():
    spark = create_spark_session()
    Bucket =config.get('AWS','BUCKET')
    clean_datasets(locations_data_df,taxi_trips_df)
    upload_files_to_s3(Bucket)
    create_spark_session()
    process_locations_data(spark)
    process_trips_data(spark)
    check_mandatory_columns()
    check_unique_columns()
    check_null_values()
    check_records()
    sql_sample(spark)

if __name__ == "__main__":
    main()
   