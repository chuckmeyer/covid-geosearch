#!python3
from algoliasearch.search_client import SearchClient
import json
import requests

# Need a way to find the latest file (date in URL)
REST_URL = 'https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/global_and_us?min_date=2021-08-30T00:00:00.000Z&max_date=2021-08-30T00:00:00.000Z&hide_fields=_id, fips, country_code, country_iso2, country_iso3, population, deaths, confirmed_daily, deaths_daily, recovered, recovered_daily'

def main():
  response = requests.get(REST_URL)

  covid_records = []
  for row in response.json():
    # Unassigned and Unknown records are alread scrubbed in this DB
    # Skip 'US' and 'Canada' since they have incomplete data
    # and locations w/o coordinates
    if row['combined_name'] != 'US' and row['combined_name'] != 'Canada' and 'loc' in row:
      covid_loc = {}
      covid_geocode = {}
      print(row['combined_name'])
      covid_loc['objectID'] = row['combined_name']
      # Let's not use the combined key for US cities, instead let's use city and state  
      if 'county' in row:
        covid_loc['location'] = row['county'] + ', ' + row['state']
      else:
        covid_loc['location'] = row['combined_name']
      covid_loc['country'] = row['country']
      covid_loc['confirmed_cases'] = row['confirmed']
      covid_geocode['lat'] = row['loc']['coordinates'][1]
      covid_geocode['lng'] = row['loc']['coordinates'][0]
      covid_loc['_geoloc'] = covid_geocode
      covid_records.append(covid_loc)
    else:
      print('Skipping {}: No geocode'.format(row['combined_name']))
  
  # Write the records to a file
  with open('export/export-rest.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  # Create the index
  client = SearchClient.create(APP_ID, API_KEY)
  index = client.init_index('covid-geo')
  settings = index.get_settings()
  with open('export/index-settings.json', 'w') as outfile:
    json.dump(settings, outfile)
  index.clear_objects()
  index.save_objects(covid_records)

if __name__ == "__main__":
    main()