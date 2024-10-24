from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, split, expr
import requests
from utility import Utils


if __name__=='__main__':

    # initialize the spark session
    spark=SparkSession.builder.master("local[*]").appName("project").getOrCreate()

    # Step 1: Get data from the URL
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = "project-earthquake-439311"
    gcp_options.region = "us"
    gcp_options.temp_location = "gs://earthquake_analysiss/temp/"
    gcp_options.job_name = "load-api-data-to-gcs"
    options.view_as(StandardOptions).runner = 'DataFlowRunner'

    response = requests.get(url)

    ## Step 1: Download the file from the URL
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    #


    # write the data where folder is:-

    output_file = 'gs://earthquake_analysiss/Dataflow/History'

    # Load Historical data by using Dataflow

    with beam.Pipeline(options=options) as p:
        res = (p | 'create Input' >> beam.Create([None])
               | "Fetch Data from API" >> beam.ParDo(utils_obj.fetch_data_from_api(), url)
               | "Write to GCS" >> beam.io.WriteToText(output_file, shard_name_template="", file_name_suffix=".json")
               )

    read the the data from silver







