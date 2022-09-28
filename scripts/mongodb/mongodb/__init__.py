#!python3
import os

from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv, find_dotenv
import json
from pymongo import MongoClient
import datetime

load_dotenv(find_dotenv())

# JHU COVID-19 Geodata ingest using MongoDB 
# https://www.mongodb.com/developer/article/johns-hopkins-university-covid-19-data-atlas/

MDB_URL = 'mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19'

export_path = "../export"
export_file = "export-mongodb.json"


def main():
  client = MongoClient(MDB_URL)
  db = client.get_database('covid19')
  stats = db.get_collection('global_and_us')
  metadata = db.get_collection('metadata')

  # Get the last date loaded:
  meta = metadata.find_one()
  last_date = meta['last_date']

  results = stats.find(
    {
      'date':last_date,
      'loc':{'$exists': True, '$ne': [] }
    }, {
      'combined_name': 1, 
      'county': 1,
      'state': 1,
      'country': 1,
      'confirmed': 1,
      'loc': 1
    }
  )

  covid_records = transform_records(results)

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
    if row['combined_name'] != 'US' and row['combined_name'] != 'Canada':
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


if __name__ == '__main__':
  main()
