#!python3
from algoliasearch.search_client import SearchClient
import pymongo
from pymongo import MongoClient
import json

MDB_URL = "mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19"

def main():
  client = MongoClient(MDB_URL)
  db = client.get_database("covid19")
  stats = db.get_collection("global_and_us")
  metadata = db.get_collection("metadata")

  # Get the last date loaded:
  meta = metadata.find_one()
  last_date = meta["last_date"]

  results = stats.find(
    {
        "date":last_date,
        "loc":{"$exists": True, "$ne": [] }
      }
  )
  # print([doc['combined_name'] + ' ' + str(doc['loc']['coordinates'][0]) + ',' + str(doc['loc']['coordinates'][1]) for doc in results])

  covid_records = []
  for row in results:
    print(row['combined_name'])
    if row['combined_name'] != 'US' and row['combined_name'] != 'Canada':
      covid_loc = {}
      covid_geocode = {}
      covid_loc['objectID'] = row['combined_name']
      covid_loc['location'] = row['combined_name']
      covid_loc['country'] = row['country']
      covid_loc['confirmed_cases'] = row['confirmed']
      covid_geocode['lat'] = row['loc']['coordinates'][1]
      covid_geocode['lng'] = row['loc']['coordinates'][0]
      covid_loc['_geoloc'] = covid_geocode
      #print(covid_loc);
      covid_records.append(covid_loc)

  # Write the records to a file
  with open('export-mongodb.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  # Create the index
  client = SearchClient.create('FMXYI0LKWR', '994555c727b589b326344e574c0a959b')
  index = client.init_index('covid-geo')
  settings = index.get_settings()
  with open('index-settings.json', 'w') as outfile:
    json.dump(settings, outfile)
  index.clear_objects()
  index.save_objects(covid_records)

if __name__ == "__main__":
    main()