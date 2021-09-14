#!python3
import os

from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv, find_dotenv
import json
import requests

load_dotenv(find_dotenv())

# JHU COVID-19 Geodata ingest using GraphQL
# https://www.mongodb.com/developer/article/johns-hopkins-university-covid-19-graphql-api/

GRAPHQL_AUTH = "https://realm.mongodb.com/api/client/v2.0/app/covid-19-qppza/auth/providers/anon-user/login"
GRAPHQL_URL  = "https://realm.mongodb.com/api/client/v2.0/app/covid-19-qppza/graphql"


def main():
  response = requests.get(GRAPHQL_AUTH)
  access_token =  response.json()['access_token']

  headers = {}
  headers["Accept"] = "application/json"
  headers["Content-Type"] = "application/json"
  headers["Authorization"] = "Bearer {}".format(access_token)

  metadata = requests.post(GRAPHQL_URL, headers=headers, json={'query': 'query { metadatum{ last_date }}'})
  if metadata.status_code != 200:
    raise Exception(f"Query failed to run with a {response.status_code}.")
  last_date = metadata.json()['data']['metadatum']['last_date']

  query = '''query {
    global_and_us(query: { date: "''' + last_date + '''" }, limit:5000)
    { confirmed county state country combined_name loc { type coordinates }}
}'''

  response = requests.post(GRAPHQL_URL, headers=headers, json={'query': query})
  if response.status_code != 200:
    raise Exception(f"Query failed to run with a {response.status_code}.")

  covid_records = transform_records(response.json()['data']['global_and_us'])

  # Write the records to a file
  with open('export/export-graphql.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  update_index(covid_records)


def transform_records(results):
  covid_records = []
  for row in results:
    # Unassigned and Unknown records are alread scrubbed in this DB
    # Skip 'US' and 'Canada' since they have incomplete data
    # and locations w/o coordinates
    if row['combined_name'] != 'US' and row['combined_name'] != 'Canada' and row['loc'] != None:
      covid_record = {}
      covid_geocode = {}
      print(row['combined_name'])
      covid_record['objectID'] = row['combined_name']
      # Let's not use the combined key for US counties, instead let's use county and state  
      if row['county'] != None:
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
