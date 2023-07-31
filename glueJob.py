import re
import sys
import boto3
import pandas as pd
import os
import io

s3Client = boto3.client('s3')
response = s3Client.get_object(Bucket='my-extract-data-buk', Key='output.csv')
df = pd.read_csv(response.get("Body"), on_bad_lines='skip')
df = df.drop(df.columns[0], axis=1)
df['Property Name'] = df['Property Name'].str.strip()
df['Address'] = df['Address'].str.strip()
df['Price'] = df['Price'].str.strip() 
df['Available Date'] = df['Available Date'].str.strip() 
df['working_dist_mrt'] = df['working_dist_mrt'].str.strip() 
df['time_posted'] = df['time_posted'].str.strip()
df['working_dist_mrt_mins'] = df['working_dist_mrt'].str.split(' mins').str[0]
pattern = r"\((.*?)\)"
df['working_dist_mrt_meter_m'] = df['working_dist_mrt'].str.extract(pattern)
df['working_dist_mrt_meter'] = df['working_dist_mrt_meter_m'].str.split(' m').str[0]
df = df.dropna()
print(df.iloc[0])

with io.StringIO() as csv_buffer:

    # TODO: write code...
	df.to_csv(csv_buffer, index=False)
    response = s3Client.put_object(
	Bucket='my-extract-data-buk', Key="transform/transformed.csv",         Body=csv_buffer.getvalue())
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

#print(df.iloc[0])
print(status)