#!python3
import os

from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv, find_dotenv
import json
import requests

load_dotenv(find_dotenv())

METADATA_URL = 'https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/metadata'
REST_URL = 'https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/global_and_us'


def main():
  meta = requests.get(METADATA_URL)
  last_date = meta.json()['last_date']
#  QUERY = {
#    'min_date': '2021-08-30T00:00:00.000Z',
#    'max_date': '2021-08-30T00:00:00.000Z',
#    'hide_fields': '_id, fips, country_code, country_iso2, country_iso3, population, deaths, confirmed_daily, deaths_daily, recovered, recovered_daily'
#  }
  QUERY = {
    'min_date': last_date,
    'max_date': last_date,
    'hide_fields': '_id, fips, country_code, country_iso2, country_iso3, population, deaths, confirmed_daily, deaths_daily, recovered, recovered_daily'
  }
  response = requests.get(REST_URL, params=QUERY)

  covid_records = transform_records(response.json())

  # Write the records to a file
  with open('export/export-rest.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  # Create the index
  client = SearchClient.create(os.getenv('APP_ID'), os.getenv('API_KEY'))
  index = client.init_index(os.getenv('ALGOLIA_INDEX_NAME'))
  settings = index.get_settings()
  with open('export/index-settings.json', 'w') as outfile:
    json.dump(settings, outfile)
  index.clear_objects()
  index.save_objects(covid_records)


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
