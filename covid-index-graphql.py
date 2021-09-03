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

  query = """query {
    global_and_us(query: { date: "2021-08-30T00:00:00Z" }, limit:5000)
    { confirmed county state country combined_name loc { type coordinates }}
}"""

  response = requests.post(GRAPHQL_URL, headers=headers, json={'query': query})
  if response.status_code != 200:
    raise Exception(f"Query failed to run with a {response.status_code}.")

  covid_records = []
  for row in response.json()['data']['global_and_us']:
    # Unassigned and Unknown records are alread scrubbed in this DB
    # Skip 'US' and 'Canada' since they have incomplete data
    # and locations w/o coordinates
    if row['combined_name'] != 'US' and row['combined_name'] != 'Canada' and row['loc'] != None:
      covid_loc = {}
      covid_geocode = {}
      print(row['combined_name'])
      covid_loc['objectID'] = row['combined_name']
      # Let's not use the combined key for US cities, instead let's use city and state  
      if row['county'] != None:
        covid_loc['location'] = row['county'] + ', ' + row['state']
      else:
        covid_loc['location'] = row['combined_name']
      covid_loc['country'] = row['country']
      covid_loc['confirmed_cases'] = row['confirmed']
      covid_geocode['lat'] = row['loc']['coordinates'][1]
      covid_geocode['lng'] = row['loc']['coordinates'][0]
      covid_loc['_geoloc'] = covid_geocode
      covid_records.append(covid_loc)

  # Write the records to a file
  with open('export/export-graphql.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  # Create the index
  client = SearchClient.create(os.getenv('APP_ID'), os.getenv('API_KEY'))
  index = client.init_index(os.getenv('ALGOLIA_INDEX_NAME'))
  settings = index.get_settings()
  with open('export/index-settings.json', 'w') as outfile:
    json.dump(settings, outfile)
  index.clear_objects()
  index.save_objects(covid_records)

if __name__ == "__main__":
    main()
