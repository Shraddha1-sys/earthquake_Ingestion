#################################################################################
# Name : Load Historical Data By Pyspark.py
# Description : Load the data after flattening
# Date : 2024 - 10 - 22
# Version : 3.3.2
# Modified date :
# Modification Description :
#################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, udf,split,lit,current_timestamp
from pyspark.sql.functions import col, explode
import json
import logging
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, LongType, \
     MapType, FloatType
from utility import Utils
from google.cloud import storage,bigquery
import os
import logging
from datetime import  *
import time

# logging.basicConfig(filename='Historical_data.log', level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':

    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Lenovo\earthquake_Ingestion\project-earthquake-439311-ce63dc0e4446.json'

    # ================================================================================

    current_date = datetime.now().strftime("%Y%m%d")  # Format the date asYYYY MMDD

    # initialize the spark session
    spark = SparkSession.builder \
          .appName("project") \
          .getOrCreate()

    # ================================================================================

    # Get the data from the API
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    utils_obj = Utils()

    response = utils_obj.fetch_data_from_api(url)

    #print(type(response))

    # ================================================================================

    # upload file into GCS
    upload_file = utils_obj.upload_to_gcs(response, 'earthquake_analysiss', f'pyspark/landing/{current_date}/Historical_data')

    # ================================================================================

    # read data from the GCS Bucket
    read_data = utils_obj.read_file_from_gcs('earthquake_analysiss', f'pyspark/landing/{current_date}/Historical_data.json')
    # print(read_data)

    # ================================================================================

    # flattening data
    flattened_data = utils_obj.flatten_earthquake_data(read_data)

    schema = utils_obj.schema()

    # # Create DataFrame
    df = spark.createDataFrame(flattened_data, schema=schema)
    #df.show()

    # ================================================================================

    # Columns like “time”, “updated” - convert its value from epoch to timestamp

    updated_df = df.withColumn('time', from_unixtime(df['time'] / 1000)).withColumn('updated', from_unixtime(df['updated'] / 1000))

    #updated_df.show()

    #updated_df.printSchema()

    # ================================================================================

    # Generate column “area” - based on existing “place” column #


    area_df=updated_df.withColumn("area", split(col("place"), " of").getItem(1))

    #area_df.show(truncate=False)

    # ================================================================================

    #Insert data : insert_dt (Timestamp)

    insert_df=area_df.withColumn('insert_dt',lit(current_timestamp()))

    #insert_df.show()

    # ================================================================================

    #write the df to GCS Bucket

    json_data = insert_df.toJSON().collect()  #json_data is a list of JSON strings

    # ================================================================================

    data_to_upload = [json.dumps(record) for record in json_data]

    write_data=utils_obj.upload_to_gcs(json_data,'earthquake_analysiss',f'silver/{current_date}/Historical_data.json')

    print(f'File written successfully ')

    print(type(json_data))

    # ================================================================================

    # upload to bigquery

    # try:
    #     insert_df.write \
    #     .format('bigquery') \
    #     .option('table', 'project-earthquake-439311.earthquake_db.earthquake_data') \
    #     .mode('overwrite') \
    #     .save()
    #
    #     print('table is created')
    #
    # except Exception as msg :
    #     print("We are facing issue to create the table at dataset")
    #     print(f'Issue is {msg}')

















