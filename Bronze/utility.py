import requests
import json
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, LongType, MapType,FloatType
from datetime import datetime


class Utils:
    def schema(self):
        flattened_schema = StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", FloatType(), True),
        StructField("mmi", FloatType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True)
    ])

        return flattened_schema


    def schema_for_bigq(self):
        flattened_schema1 = StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", FloatType(), True),
        StructField("mmi", FloatType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True)
    ])

        return flattened_schema1


    def fetch_data_from_api(self, url):
        """

        :param url: URL which mentioned hostorical
        :return: return the data into the json format
        """
        try:
            response = requests.get(url)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    def flatten_earthquake_data(self,even_data):
        """

        :param even_data: data which needs to be flattened
        :return: return flatted data into list of dict
        """

        flattened_data=[]

        features=even_data.get('features',[])

        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            coordinates = geometry["coordinates"]

            flat_event = {
                "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
                "place": properties.get("place"),
                "time": properties.get("time"),
                "updated": properties.get("updated"),
                "tz": properties.get("tz"),
                "url": properties.get("url"),
                "detail": properties.get("detail"),
                "felt": properties.get("felt"),
                "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
                "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
                "alert": properties.get("alert"),
                "status": properties.get("status"),
                "tsunami": properties.get("tsunami"),
                "sig": properties.get("sig"),
                "net": properties.get("net"),
                "code": properties.get("code"),
                "ids": properties.get("ids"),
                "sources": properties.get("sources"),
                "types": properties.get("types"),
                "nst": properties.get("nst"),
                "dmin": float(properties.get("dmin")) if properties.get("dmin") is not None else None,
                "rms": float(properties.get("rms")) if properties.get("rms") is not None else None,
                "gap": float(properties.get("gap")) if properties.get("gap") is not None else None,
                "magType": properties.get("magType"),
                "type": properties.get("type"),
                "title": properties.get("title"),
                "longitude": coordinates[0],
                "latitude": coordinates[1],
                "depth": float(coordinates[2]) if coordinates[2] is not None else None
            }

            flattened_data.append(flat_event)

        return flattened_data
    def upload_to_gcs(self, data, bucket_name, blob_name):

        """

        :param data:
        :param bucket_name:
        :param blob_name:
        :return:
        """

        client = storage.Client()

        # Get the bucket and blob objects
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Convert the dict to JSON string for uploading
        json_data = json.dumps(data)

        # Upload the content directly from the response
        blob.upload_from_string(json_data, content_type='application/json')

        return print('successfully uploaded')

    #Read the file from the GCS Bucket
    def read_file_from_gcs(self,bucket_name,blob_name):

        client = storage.Client()

        bucket=client.bucket(bucket_name)

        blob=bucket.blob(blob_name)

        json_data = blob.download_as_text()

        data = json.loads(json_data)  # Parse JSON into a Python dictionary

        return data


# class FetchDataFromAPI(beam.DoFn):
#     def process(self, url):
#         response = requests.get(url)
#         return response.json()
