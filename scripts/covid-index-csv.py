#!python3
import os

from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv, find_dotenv
import json
import pandas

load_dotenv(find_dotenv())

# Need a way to find the latest file (date string filenames)
DATA_FILE = '../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/08-30-2021.csv'


def main():
  df = pandas.read_csv(DATA_FILE)

  covid_records = transform_records(df.iterrows())

  # Write the records to a file
  with open('export/export-csv.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  update_index(covid_records)


def transform_records(results):
  covid_records = []
  for index, row in results:
    # Skip locations w/o coordinates
    if pandas.isna(row['Lat']):
      print('Skipping {}: No geocode'.format(row['Combined_Key']))
    else:
      covid_record = {}
      covid_geocode = {}
      print(row['Combined_Key'])
      covid_record['objectID'] = row['Combined_Key']
      # Let's not use the combined key for US counties, instead let's use county and state  
      if pandas.isna(row['Admin2']):
        covid_record['location'] = row['Combined_Key']
      else:
        covid_record['location'] = row['Admin2'] + ', ' + row['Province_State']
      covid_record['country'] = row['Country_Region']
      covid_record['confirmed_cases'] = int(row['Confirmed'])
      covid_geocode['lat'] = row['Lat']
      covid_geocode['lng'] = row['Long_']
      covid_record['_geoloc'] = covid_geocode
      covid_records.append(covid_record)
  return covid_records


def update_index(covid_records):
  # Create the index
  client = SearchClient.create(os.getenv('APP_ID'), os.getenv('API_KEY'))
  index = client.init_index(os.getenv('ALGOLIA_INDEX_NAME'))
  index.clear_objects()
  index.save_objects(covid_records)


if __name__ == "__main__":
  main()
