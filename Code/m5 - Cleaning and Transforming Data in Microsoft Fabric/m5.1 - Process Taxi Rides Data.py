#!/usr/bin/env python
# coding: utf-8

# ## Process Taxi Rides Data
# 
# New notebook

# ### Create DataFrame by reading a file

# In[ ]:


yellowTaxisFilePath = "abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/TaxiRidesDataLake/taxidata/Raw/YellowTaxis_202405.csv"


# In[ ]:


# Create DataFrame

yellowTaxiDF = (
                    spark
                        .read
                        .csv(yellowTaxisFilePath)
                )

# Print schema
yellowTaxiDF.printSchema()

# Display data
display(yellowTaxiDF)       #yellowTaxiDF.show()


# In[ ]:


# Create DataFrame

yellowTaxiDF = (
                    spark
                        .read

                        .option("header", "true")

                        .csv(yellowTaxisFilePath)
                )

# Print schema
yellowTaxiDF.printSchema()

# Display data
display(yellowTaxiDF)       #yellowTaxiDF.show()


# ### Read JSON File

# In[ ]:


# Read PaymentTypes JSON file

paymentTypesDF = (
                      spark
                        .read
                        .json("abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/TaxiFilesUpload/PaymentTypes*.json")
                 )

#paymentTypesDF = (
#                     spark
#                       .read

#                       .format("json")

#                       .load("abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/TaxiFilesUpload/PaymentTypes*.json")
#                 )

display(paymentTypesDF)


# ### Schema Option 1 - No schema inference or definition

# In[ ]:


# Create DataFrame

yellowTaxiDF = (
                    spark
                        .read
                        .option("header", "true")

                        .csv(yellowTaxisFilePath)
                )

# Print schema
yellowTaxiDF.printSchema()


# ### Schema Option 2 - Infer schema

# In[ ]:


# Create DataFrame by inferring the schema

yellowTaxiDF = (
                    spark
                        .read
                        .option("header", "true")

                        .option("inferSchema", "true")

                        .csv(yellowTaxisFilePath)
                )

# Print schema
yellowTaxiDF.printSchema()


# ### Schema Option 3 - Define schema & apply

# In[ ]:


from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create schema for Yellow Taxi Data

yellowTaxiSchema = (
                        StructType
                        ([ 
                            StructField("VendorId"               , IntegerType()   , True),
                            StructField("lpep_pickup_datetime"   , TimestampType() , True),
                            StructField("lpep_dropoff_datetime"  , TimestampType() , True),
                            StructField("passenger_count"        , DoubleType()    , True),
                            StructField("trip_distance"          , DoubleType()    , True),
                            StructField("RatecodeID"             , DoubleType()    , True),
                            StructField("store_and_fwd_flag"     , StringType()    , True),
                            StructField("PULocationID"           , IntegerType()   , True),
                            StructField("DOLocationID"           , IntegerType()   , True),
                            StructField("payment_type"           , IntegerType()   , True),
                            StructField("fare_amount"            , DoubleType()    , True),
                            StructField("extra"                  , DoubleType()    , True),
                            StructField("mta_tax"                , DoubleType()    , True),
                            StructField("tip_amount"             , DoubleType()    , True),
                            StructField("tolls_amount"           , DoubleType()    , True),
                            StructField("improvement_surcharge"  , DoubleType()    , True),
                            StructField("total_amount"           , DoubleType()    , True),
                            StructField("congestion_surcharge"   , DoubleType()    , True),
                            StructField("airport_fee"            , DoubleType()    , True)
                        ])
                   )


# In[ ]:


# Create DataFrame by applying the schema

yellowTaxiDF = (
                    spark
                        .read
                        .option("header", "true")

                        .schema(yellowTaxiSchema)

                        .csv(yellowTaxisFilePath)
                )

# Print schema
yellowTaxiDF.printSchema()


# ### Define schema for JSON file

# In[ ]:


# Create DataFrame by reading JSON file

taxiBasesDF = (
                  spark
                    .read

                    .option("multiline", "true")

                    .json("abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/TaxiFilesUpload/TaxiBases.json")
              )

# Print schema
taxiBasesDF.printSchema()

# Display data
display(taxiBasesDF) 


# In[ ]:


# Define schema for JSON data

taxiBasesSchema = (
                    StructType
                    ([
                        StructField("License Number"         , StringType()    , True),
                        StructField("Entity Name"            , StringType()    , True),
                        StructField("Telephone Number"       , LongType()      , True),
                        StructField("SHL Endorsed"           , StringType()    , True),
                        StructField("Type of Base"           , StringType()    , True),

                        StructField("Address", 
                                        StructType
                                        ([
                                            StructField("Building"   , StringType(),   True),
                                            StructField("Street"     , StringType(),   True), 
                                            StructField("City"       , StringType(),   True), 
                                            StructField("State"      , StringType(),   True), 
                                            StructField("Postcode"   , StringType(),   True)
                                        ]),
                                    True
                                   ),
                        
                        StructField("GeoLocation", 
                                        StructType
                                        ([
                                            StructField("Latitude"   , StringType(),   True),
                                            StructField("Longitude"  , StringType(),   True), 
                                            StructField("Location"   , StringType(),   True)
                                        ]),
                                    True
                                   )  
                  ])
                )


# In[ ]:


# Create DataFrame by reading JSON file

taxiBasesDF = (
                  spark
                    .read
                    .option("multiline", "true")

                    .schema(taxiBasesSchema)

                    .json("abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/TaxiFilesUpload/TaxiBases.json")
              )

# Print schema
taxiBasesDF.printSchema()

# Display data
display(taxiBasesDF) 


# ### Analyze Data

# In[ ]:


yellowTaxiAnalyzedDF = (
                            yellowTaxiDF.describe
                            (
                                "passenger_count",
                                "trip_distance"
                            )
                       )

display(yellowTaxiAnalyzedDF)


# ### Clean Data

# #### 1. Accuracy Check: Filter inaccurate data

# In[ ]:


# Display the count before operation
print("Before operation = " + str( yellowTaxiDF.count()) )


yellowTaxiDF = (
                  yellowTaxiDF
    
                      .where("passenger_count > 0")

                      .filter(col("trip_distance") > 0.0)
               )


# Display the count after operation
print("After operation = " + str( yellowTaxiDF.count()) )


# #### 2.a. Completeness Check: Drop rows with nulls

# In[ ]:


# Display the count before operation
print("Before operation = " + str( yellowTaxiDF.count()) )


yellowTaxiDF = (
                   yellowTaxiDF    
                          .na.drop('all')
               )


# Display the count after operation
print("After operation = " + str( yellowTaxiDF.count()) )


# #### 2.b. Completeness Check: Replace nulls with default values

# In[ ]:


defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}


yellowTaxiDF = (
                   yellowTaxiDF    
                      .na.fill(defaultValueMap)
               )


# #### 3. Uniqueness Check: Drop duplicate rows

# In[ ]:


# Display the count before operation
print("Before operation = " + str( yellowTaxiDF.count()) )


yellowTaxiDF = (
                   yellowTaxiDF
                          .dropDuplicates()
               )


# Display the count after operation
print("After operation = " + str( yellowTaxiDF.count()) )


# #### 4. Timeliness Check: Remove records outside the bound

# In[ ]:


# Display the count before operation
print("Before operation = " + str( yellowTaxiDF.count()) )


yellowTaxiDF = (
    
     yellowTaxiDF
        .where("lpep_pickup_datetime >= '2024-05-01' AND lpep_dropoff_datetime < '2024-06-01'")
)


# Display the count after operation
print("After operation = " + str( yellowTaxiDF.count()) )


# ### Chain all cleanup operation together

# In[ ]:


defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}

# Read file
yellowTaxiDF = (
                  spark
                    .read
                    .option("header", "true")    
                    .schema(yellowTaxiSchema)    
                    .csv(yellowTaxisFilePath)
               )

# Cleanup data by applying data quality checks
yellowTaxiDF = (
                  yellowTaxiDF    
    
                      .where("passenger_count > 0")
    
                      .filter(col("trip_distance") > 0.0)
                          
                      .na.drop('all')
    
                      .na.fill(defaultValueMap)
    
                      .dropDuplicates()
    
                      .where("lpep_pickup_datetime >= '2024-05-01' AND lpep_dropoff_datetime < '2024-06-01'")
               )

# Display the count after operation
print("After operation = " + str( yellowTaxiDF.count()) )


# In[ ]:


yellowTaxiDF.printSchema()


# ### Transform Data

# #### 1. Select limited columns

# In[ ]:


yellowTaxiDF = (
                   yellowTaxiDF

                        # Select only limited columns
                        .select(
                                  "VendorID",
                             
                                  col("passenger_count").cast(IntegerType()),
                            
                                  column("trip_distance").alias("TripDistance"),
                            
                                  yellowTaxiDF.lpep_pickup_datetime,
                            
                                  "lpep_dropoff_datetime",
                                  "PUlocationID",
                                  "DOlocationID",
                                  "RatecodeID",
                                  "total_amount",
                                  "payment_type"
                               )
    
                        # Don't run, since airport_fee has not been selected above    
                        # .drop("airport_fee") 
               )

yellowTaxiDF.printSchema()


# #### 2. Rename columns

# In[ ]:


yellowTaxiDF = (
                   yellowTaxiDF                        
                        
                        .withColumnRenamed("passenger_count", "PassengerCount")
    
                        .withColumnRenamed("lpep_pickup_datetime", "PickupTime")
                        .withColumnRenamed("lpep_dropoff_datetime", "DropTime")
                        .withColumnRenamed("PUlocationID", "PickupLocationId")
                        .withColumnRenamed("DOlocationID", "DropLocationId")
                        .withColumnRenamed("total_amount", "TotalAmount")
                        .withColumnRenamed("payment_type", "PaymentType")    
               )

yellowTaxiDF.printSchema()


# #### 3.a. Create derived columns - TripYear, TripMonth, TripDay

# In[ ]:


# Create derived columns for year, month and day
yellowTaxiDF = (
                  yellowTaxiDF
    
                        .withColumn("TripYear", year(col("PickupTime")))
    
                        .select(
                                    "*",
                            
                                    expr("month(PickupTime) AS TripMonth"),
                            
                                    dayofmonth(col("PickupTime")).alias("TripDay")
                               )
               )

yellowTaxiDF.printSchema()


# #### 3.b. Create derived column - TripTimeInMinutes

# In[ ]:


tripTimeInSecondsExpr = unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))


tripTimeInMinutesExpr = round(tripTimeInSecondsExpr / 60)


yellowTaxiDF = (
                  yellowTaxiDF
                        .withColumn("TripTimeInMinutes", tripTimeInMinutesExpr)
               )

yellowTaxiDF.printSchema()


# #### 3.c. Create derived column - TripType

# In[ ]:


tripTypeColumn = (
                    when(
                            col("RatecodeID") == 6,
                              "SharedTrip"
                         )
                    .otherwise("SoloTrip")
                 )    


yellowTaxiDF = (
                  yellowTaxiDF
    
                        .withColumn("TripType", tripTypeColumn)
               )

yellowTaxiDF.printSchema()


# ### Read JSON file using schema

# In[ ]:


taxiBasesSchema = (
                    StructType
                    ([
                        StructField("License Number"         , StringType()    , True),
                        StructField("Entity Name"            , StringType()    , True),
                        StructField("Telephone Number"       , LongType()      , True),
                        StructField("SHL Endorsed"           , StringType()    , True),
                        StructField("Type of Base"           , StringType()    , True),

                        StructField("Address", 
                                        StructType
                                        ([
                                            StructField("Building"   , StringType(),   True),
                                            StructField("Street"     , StringType(),   True), 
                                            StructField("City"       , StringType(),   True), 
                                            StructField("State"      , StringType(),   True), 
                                            StructField("Postcode"   , StringType(),   True)
                                        ]),
                                    True
                                   ),
                        
                        StructField("GeoLocation", 
                                        StructType
                                        ([
                                            StructField("Latitude"   , StringType(),   True),
                                            StructField("Longitude"  , StringType(),   True), 
                                            StructField("Location"   , StringType(),   True)
                                        ]),
                                    True
                                   )  
                  ])
                )


# In[ ]:


taxiBasesDF = (
                  spark
                    .read    
                    .option("multiline", "true")
    
                    .schema(taxiBasesSchema)
    
                    .json("abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/TaxiFilesUpload/TaxiBases.json")
              )

display(taxiBasesDF)


# ### Extract nested fields from JSON

# In[ ]:


taxiBasesFlatDF = (
                        taxiBasesDF
                            .select(
                                      col("License Number").alias("BaseLicenseNumber"),
                                      col("Entity Name").alias("EntityName"),

                                      col("Address.Building").alias("AddressBuilding"),

                                      col("Address.Street").alias("AddressStreet"),
                                      col("Address.City").alias("AddressCity"),
                                      col("Address.State").alias("AddressState"),
                                      col("Address.Postcode").alias("AddressPostCode"),

                                      col("GeoLocation.Latitude").alias("GeoLatitude"),
                                      col("GeoLocation.Longitude").alias("GeoLongitude")
                                   )
                  )

display(taxiBasesFlatDF)


# ### Aggregate Data

# In[ ]:


yellowTaxiDFReport = (
                         yellowTaxiDF
    
                                .groupBy( "PickupLocationId", "DropLocationId" )
    
                                .agg( 
                                        avg("TripTimeInMinutes").alias("AvgTripTime"),
                                    
                                        sum("TotalAmount").alias("SumAmount")
                                    )
    
                                .orderBy( col("PickupLocationId").desc() )
                     )

display(yellowTaxiDFReport)


# ### Using Spark SQL

# In[ ]:


yellowTaxiDF.createOrReplaceTempView("YellowTaxis")


# In[ ]:


outputDF = spark.sql(
                        "SELECT * FROM YellowTaxis WHERE PickupLocationId = 171"
                    )
    
display(outputDF)


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nSELECT * \nFROM YellowTaxis \nWHERE PickupLocationId = 171\n')


# ### Save data

# #### 1. Save in CSV format to Files section

# In[ ]:


(
    yellowTaxiDFReport    
            .write
            
            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    
            .mode("overwrite")                      # Options - Append, Overwrite, ErrorIfExists, Ignore
    
            .csv("Files/YellowTaxisReport.csv")     # Options - csv, json, parquet, jdbc, etc.
)


# #### 2. Save in Delta format as an External Table

# In[ ]:


(
    yellowTaxiDF
            .write

            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")

            .format("delta")
    
            .mode("overwrite")

            .option("path", "Files/YellowTaxisUnmanaged.delta")
    
            .saveAsTable("SilverLakehouse.YellowTaxisUnmanaged")
)


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nDESCRIBE EXTENDED YellowTaxisUnmanaged\n')


# #### 3. Save in Delta format as a Managed Table

# In[ ]:


(
    yellowTaxiDF
            .write

            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")

            .format("delta")
    
            .mode("overwrite")

            .saveAsTable("SilverLakehouse.YellowTaxis")
)


# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nDESCRIBE EXTENDED YellowTaxis\n')


# ### Check Audit History of Delta Table

# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nDESCRIBE HISTORY YellowTaxis\n')


# ### Create other tables in Silver Lakehouse

# #### 1. Payment Types
# 
# <i> By using a DataFrame based on CSV file

# In[ ]:


(
    paymentTypesDF
            .write
            .option("header", "true")
            .format("delta")    
            .mode("overwrite")
            .saveAsTable("PaymentTypes")
)


# #### 2. Taxi Bases
# 
# <i> By using a DataFrame based on JSON file

# In[ ]:


(
    taxiBasesFlatDF
            .write
            .option("header", "true")
            .format("delta")    
            .mode("overwrite")
            .saveAsTable("TaxiBases")
)


# #### 3. Customers
# 
# <i> By reading file from other Lakehouse

# In[ ]:


(
    spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("abfss://TaxiRidesWorkspace@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Files/CustomerData/Customers.csv")

        .write
        .option("header", "true")
        .format("delta")    
        .mode("overwrite")
        .saveAsTable("Customers")
)


# #### 4. Taxi Zones and Cabs
# 
# <i> By reading tables from other Lakehouse

# In[ ]:


(
    spark
        .read
        .table("BronzeLakehouse.TaxiZones")
        
        .write
        .format("delta")    
        .mode("overwrite")
        .saveAsTable("TaxiZones")
)

(
    spark
        .read
        .table("BronzeLakehouse.Cabs")
        
        .write
        .format("delta")    
        .mode("overwrite")
        .saveAsTable("Cabs")
)


# In[ ]:




