#################################################################################
# Name : Load Historical Data By Pyspark.py
# Description : Load the data after flattening
# Date : 2024 - 10 - 22
# Version : 3.3.2
# Modified date :
# Modification Description :
#################################################################################

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, udf
from pyspark.sql.functions import col, explode
import json
import logging
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, LongType, \
    MapType, FloatType
from utility import Utils
from google.cloud import storage
import apache_beam as beam
import os

if __name__ == '__main__':

    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Lenovo\Project_First\.venv\project-earthquake-439311-29bcc5e29e30.json'

    # ================================================================================

    # initialize the spark session
    spark = SparkSession.builder.master("local[*]").appName("project").getOrCreate()

    # ================================================================================

    # Get the data from the API
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    utils_obj = Utils()

    response = utils_obj.fetch_data_from_api(url)

    print(type(response))

    # ================================================================================

    # upload file into GCS
    upload_file = utils_obj.upload_to_gcs(response, 'earthquake_analysiss', 'pyspark/landing/20241022/Historical_data')

    # read data from the GCS Bucket

    # ================================================================================

    read_data = utils_obj.read_file_from_gcs('earthquake_analysiss', 'pyspark/landing/20241022/Historical_data.json')
    # print(read_data)

    # ================================================================================

    # flattening data
    flattened_data = utils_obj.flatten_earthquake_data(read_data)

    schema = utils_obj.schema()
    #
    # # Create DataFrame
    df = spark.createDataFrame(flattened_data, schema=schema)
    df.show()

    # ================================================================================

    # Columns like “time”, “updated” - convert its value from epoch to timestamp

    updated_df = df.withColumn('time', from_unixtime(df['time'] / 1000)).withColumn('updated', from_unixtime(df['updated'] / 1000))

    updated_df.show()

    updated_df.printSchema()

    # ================================================================================

    # Generate column “area” - based on existing “place” column
    #
    def city(place):
        if place:
            words = place.split()
            return words[-1]  # Return the last word
            return None

    ek_function = udf(city, StringType())

    area_df = updated_df.withColumn('area', ek_function(col('place')))

    area_df.show(truncate=False)

    #write the df to GCS Bucket

    json_data = df.toJSON().collect()  #json_data is a list of JSON strings

    # upload_to_gcs1=utils_obj.upload_to_gcs(area_df,'earthquake_analysiss','silver/historical_data')
    # print('Sucessfully Written')

    data_to_upload = [json.loads(record) for record in json_data]

    write_data=utils_obj.upload_to_gcs(data_to_upload,'earthquake_analysiss','silver/20241022/Historical_data')

    print(f'File written successfully {write_data}')

