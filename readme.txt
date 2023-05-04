project Scope 

in this project we aim to provide star schema in amazon redshift. 
so data analytics can use it to analyze the revenues
, predict trip durations or locations, predict payments types & passengers attributes
datasets used here are one dataset for trips data and one for locations data.

this project will be done using python scripts( beacuase its faster and simple although i like 
to see and test  the results of every code continuously i prefered jupyter notebook in some cases)
, and pandas library to read and clean data ( i prefer to clean data using pandas becuase i believe pandas
 is the most powerful tool to wrangle, assess,clean and modify data & also becuase dropping null values with
 spark removes all the data in trips dataset),upload clean data to s3 bucket using boto3 
then pyspark should read the new clean data from s3 bucket and create the data model ( star schema )


project summury :
this project aim to gather data about green taxi trips in NYC for year 2020 and ETL them to provide star schema
for data analysis team so they can use it to predict trip durations crowded zones and payments types


Project Data : 

The Project full data is divided into 2 Datasets:
1- 2020 Green Taxi Trip Data
The records are generated from the trip record submissions made by green taxi Technology Service Providers (TSPs). 
Each row represents a single trip in a green taxi in 2020. 
The trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, 
trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

 
this data can be found here : https://catalog.data.gov/dataset/2020-green-taxi-trip-data-january-june
2- NYC Taxi Zones 
This map shows the NYC Taxi Zones, which correspond to the pickup and drop-off zones, 
or LocationIDs, included in the Yellow, Green, and FHV Trip Records published to Open Data.
this data can be found here :https://catalog.data.gov/dataset/nyc-taxi-zones

data dictionary : 

the data is devided to 2 datasets both were downloaded from data.gov and both are about green taxi trips 

dataset 1 : green taxi trips data for 2020

it contains these columns:

1) Administrative data:

VendorID
Store_and_fwd_flag
RateCodeID

2) Trip data:

lpep_pickup_datetime
lpep_dropoff_datetime
Passenger_count
Trip_distance
Trip_type

3) Payment data:

Fare_amount
Extra, MTA_tax
Tip_amount
Tolls_amount
Ehail_fee
Improvement_surcharge
Total_amount
Payment_type

dataset 2 :

locations data : the data specified to the locations

it contains these columns :

LocationID 
Borough 
Zone  
service_zone 
latitude 
longitude      

columns description 
taxi_trips_data : 
_______________________________________________________________________________________________________
            |           |        | Values   |                                                          |
   Name     | Data type | Unique | (sample) |                Description                               |          
____________|___________|________|__________|__________________________________________________________|   
vendorID    |           |   2    |          | A code indicating LPEP provider that provided the record.| 
            |  float64  |        | (2 , 1)  | 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.   |
____________|___________|________|__________|__________________________________________________________|
storeAnd-   |           |        |          | indicates whether the trip record was held in vehicle    |
FwdFlag     |  object   |   2    | ( N,Y )  | memory  before  sending to the vendor,                   |
            |           |        |          | Y= store and forward trip N= not a store and forward trip|
____________|___________|________|__________|__________________________________________________________|
            |           |        |          |  final rate code in effect at the end of the trip.       |
rateCodeID  |  float64  |   8    | (1 , 5)  | 1= Standard rate 2= JFK 3= Newark                        |
            |           |        |          | 4= Nassau or Westchester 5= Negotiated fare 6= Group ride|
____________|___________|________|__________|__________________________________________________________|
lpep_pickup_|           |        |2013\10\22|                                                          |
datetime    | DateTime  |1366194 |- 12:40:36|   The date and time when the meter was engaged.          |
____________|___________|________|__________|__________________________________________________________|
lpep_dropoff|           |        |2013\10\22|                                                          |
_datetime   | DateTime  |1366295 |- 12:54:41|   The date and time when the meter was disengaged.       |
____________|___________|________|__________|__________________________________________________________|
PULocationID|   int64   |  260   |( 74, 42 )|   TLC Taxi Zone in which the taximeter was engaged.      | 
____________|___________|________|__________|__________________________________________________________| 
DOLocationID|   int64   |  260   |( 71, 143)|   TLC Taxi Zone in which the taximeter was disengaged.   |
____________|___________|________|__________|__________________________________________________________|
passengers  |  float64  |  11    |  (1 , 5) |   The number of passengers in the vehicle.               |
____________|___________|________|__________|__________________________________________________________|
distance    |  float64  | 4607   |(0.9,1.0) |   The elapsed trip distance in miles                     |
____________|___________|________|__________|__________________________________________________________|  
fare_amount |  float64  | 7890   |(6.0,5.5) |   The time-and-distance fare calculated by the meter.    |
____________|___________|________|__________|__________________________________________________________|
extra       |  float64  |   74   | (0.5,1)  |       Miscellaneous extras and surcharges.               |
____________|___________|________|__________|__________________________________________________________|
mta_tax     |  float64  |   5    |(0.5,-0.5)| automatically triggered based on the metered rate in use.|
____________|___________|________|__________|__________________________________________________________|
tip_amount  |  float64  |  2146  |   4.06   |  automatically populated for credit card tips only.      |
____________|___________|________|__________|__________________________________________________________|
tolls_amount|  float64  |  9598  | (5.54    |     Total amount of all tolls paid in trip.              |
____________|___________|________|__________|__________________________________________________________|
ehail_fee   |  float64  |   1    |  NAN     | this column is empty and shall be removed                |
____________|___________|________|__________|__________________________________________________________|
improvement_|           |        |          |   improvement surcharge assessed on hailed               |
surcharge   |  float64  |   3    |   0.3    |   trips at the flag drop.                                |
____________|___________|________|__________|__________________________________________________________|
total_amount|  float64  |  9598  |(7.8,6.8) |  The total amount charged to passengers.                 |
____________|___________|________|__________|__________________________________________________________|
            |           |        |          | Numeric code signifying how the passenger paid for trip. |
payment_type|  float64  |   6    |  (1,2)   |  1= Credit card 2= Cash 3= No charge 4= Dispute          |
            |           |        |          |  5= Unknown 6= Voided trip                               |
____________|___________|________|__________|__________________________________________________________|
            |           |        |          | code indicating whether the trip was a street-hail or    |
            |           |        |          | a dispatch that is automatically assigned based on the   |
trip_type   |  float64  |   3    |   (1,2)  | metered rate in use but can be altered by the driver.    |
            |           |        |          | 1= Street-hail 2= Dispatch                               |
____________|___________|________|__________|__________________________________________________________|
congestion_ |           |        |          |  The surcharge is generally: $2.75 for each for-hire     |
surcharge   |  float64  |   6    |    2.75  |  transportation trip in a vehicle that is not a medallion|
            |           |        |          |  taxicab or a pool vehicle                               |
____________|___________|________|__________|__________________________________________________________|



locations data columns :

_____________________________________________________________________________________
            |           |        |   Values     |                                    |
   Name     | Data type | Unique |   (sample)   |          Description               |          
____________|___________|________|______________|____________________________________| 
LocationID  |   INT     |  265   |   ( 1,2 )    | Unique ID refers to a location     |
____________|___________|________|______________|____________________________________|
 Borough    |  object   |   7    |    Bronx     |   location's borough               |
____________|___________|________|______________|____________________________________|
  Zone      |  object   |  262   |  Ridgewood   |   location's zone                  |            
____________|___________|________|______________|____________________________________|
service_zone|  object   |   5    |  Boro Zone   |   location's service_zone          |
____________|___________|________|______________|____________________________________| 
latitude    |  float    |  251   | 40.81421187  |   location's latitude              |
____________|___________|________|______________|____________________________________|
longitude   |  float    |  250   | -73.984016   |   location's longitude             |
____________|___________|________|______________|____________________________________|    


project approach :
data was downloaded manualy to local machine then loaded to jupyter notebook by pandas
data was cleaned and manipulated by pandas
clean data was uploaded to s3 bucket by boto3 library
clean data was loaded from s3 bucket by pyspark
data warehousing was made by pyspark

data model schema : 

 _______________________                                      __________________________
 |  trips table (dim)   |                                     |  locations table (dim)  |
 |______________________|     _________________________       |_________________________| 
 |    TripID            |     |  payments table(fact)  |      |       LocationID        |
 |    pickup_time       |     |________________________|      |        Borough          |
 |    dropoff_time      |     |     TripID             |      |        Zone             |
 |    pickup_location   |     |     fare_amount        |      |        service_zone     |
 |    dropoff_location  |     |     extra              |      |        latitude         |
 |    distance          |     |     mta_tax            |      |        longitude        |
 |    passengers        |     |     tip_amount         |      |_________________________|
 |    trip_type         |     |     tolls_amount       |      _______________________                       
 |______________________|     |  improvement_surcharge |      |  admin table  (dim) |
                              |      total_amount      |      |_____________________|               
                              |      payment_type      |      |  TripId             |
                              |      trip_type         |      |  VendorID           |  
                              |  congestion_surcharge  |      |  storeAndFwdFlag    |                           
                              |________________________|      |  RatecoedeID        |                            
                                                              |_____________________|                 

possible scenarios 
  * The data was increased by 100x.
    Apache Spark is the tool for big data processing, and is already used as the project analityc tool.
    it will not be an issue but we still need to create EMR cluster as the infrastructure that holds the
    spark is still important
   
  * The data populates a dashboard that must be updated on a daily basis by 7am every day.   
    in this case apache airflow is required 
    A DAG that trigger Phython scripts and Spark jobs executions, 
    needs to be scheduled for daily execution at 7am.         

  * The database needed to be accessed by 100+ people.
    Horizontal scaling at the application layer, 
    RDS Multi-AZ, and RDS Read Replicas will allow system to scale pretty far.


a sql query example was writen to Provide evidence that the ETL has processed the result
 in the final data model.

 
sql_query_example : 
" this should return the tripID , pickup & dropoff locations , 
  distance and total amount paid for trips above 10 miles "


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
      limit 10;



result of the query shown :

+------+--------------------+--------------------+--------+------------+                                              
|TripID|     pickup_location|    dropoff_location|distance|total_amount|
+------+--------------------+--------------------+--------+------------+
|  3506|  Murray Hill-Queens|           Ridgewood|   10.99|        32.8|
|  7225|   East Harlem North|         JFK Airport|   16.91|       73.42|
| 10871|Van Cortlandt Vil...|East New York/Pen...|   23.93|        73.3|
| 27651|             Astoria|        Bedford Park|   11.18|        47.9|
| 37310|        Forest Hills|       Alphabet City|    10.4|       47.55|
| 48603|            Elmhurst|             Jamaica|   12.64|        39.8|
| 52743| Morningside Heights|         JFK Airport|   18.07|       67.92|
| 55426|     Jackson Heights|         JFK Airport|    10.7|         0.0|
| 58768|       Melrose South|       Richmond Hill|   13.26|       45.92|
| 69549|   East Harlem North|    Bensonhurst West|   20.72|        63.8|
+------+--------------------+--------------------+--------+------------+