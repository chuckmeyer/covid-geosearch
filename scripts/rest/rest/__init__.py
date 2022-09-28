#!python3
import os

from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv, find_dotenv
import json
import requests

load_dotenv(find_dotenv())

# JHU COVID-19 Geodata ingest using REST API

METADATA_URL = 'https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/metadata'
REST_URL = 'https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/global_and_us'

export_path = "../export"
export_file = "export-rest.json"

def main():
  metadata = requests.get(METADATA_URL)
  if metadata.status_code != 200:
    raise Exception(f"Query failed to run with a {response.status_code}.")
  last_date = meta.json()['last_date']

  query = {
    'min_date': last_date,
    'max_date': last_date,
    'hide_fields': '_id, fips, country_code, country_iso2, country_iso3, population, deaths, confirmed_daily, deaths_daily, recovered, recovered_daily'
  }

  response = requests.get(REST_URL, params=query)
  if response.status_code != 200:
    raise Exception(f"Query failed to run with a {response.status_code}.")

  covid_records = transform_records(response.json())

  # Write the records to a file
  if not os.path.exists(export_path):
    os.makedirs(export_path)
  
  with open(os.path.join(export_path, export_file), 'w') as outfile:
    json.dump(covid_records, outfile)

  update_index(covid_records)


def transform_records(results):
  covid_records = []
  for row in results:
    # Unassigned and Unknown records are alread scrubbed in this DB
    # Skip 'US' and 'Canada' since they have incomplete data
    # and locations w/o coordinates
    if row['combined_name'] != 'US' and row['combined_name'] != 'Canada' and 'loc' in row:
      covid_record = {}
      covid_geocode = {}
      print(row['combined_name'])
      covid_record['objectID'] = row['combined_name']
      # Let's not use the combined key for US counties, instead let's use county and state  
      if 'county' in row:
        covid_record['location'] = row['county'] + ', ' + row['state']
      else:
        covid_record['location'] = row['combined_name']
      covid_record['country'] = row['country']
      covid_record['confirmed_cases'] = row['confirmed']
      covid_geocode['lat'] = row['loc']['coordinates'][1]
      covid_geocode['lng'] = row['loc']['coordinates'][0]
      covid_record['_geoloc'] = covid_geocode
      covid_records.append(covid_record)
    else:
      print('Skipping {}: No geocode'.format(row['combined_name']))
  return covid_records


def update_index(covid_records):
  # Create the index
  client = SearchClient.create(os.getenv('APP_ID'), os.getenv('API_KEY'))
  index = client.init_index(os.getenv('ALGOLIA_INDEX_NAME'))
  index.clear_objects()
  index.save_objects(covid_records)


if __name__ == "__main__":
  main()
