#!python3
from algoliasearch.search_client import SearchClient
import json
import pandas

DATA_FILE = '../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/08-30-2021.csv'

def main():
  df = pandas.read_csv(DATA_FILE)
  df = df.fillna(0)

  # Need a way to find the latest file (date string filenames)

  covid_records = []
  for index, row in df.iterrows():
    # Skip locations w/o coordinates
    if row['Lat'] == 0:
      print('Skipping {}: No geocode'.format(row['Combined_Key']))
    else:
      covid_loc = {}
      covid_geocode = {}
      covid_loc['objectID'] = row['Combined_Key']
      # Let's not use the combined key for US cities, instead let's use city and state  
      if row['Admin2'] != 0:
        covid_loc['location'] = row['Admin2'] + ', ' + row['Province_State']
      else:
        covid_loc['location'] = row['Combined_Key']
      covid_loc['country'] = row['Country_Region']
      covid_loc['confirmed_cases'] = int(row['Confirmed'])
      covid_geocode['lat'] = row['Lat']
      covid_geocode['lng'] = row['Long_']
      covid_loc['_geoloc'] = covid_geocode
      covid_records.append(covid_loc)

  # Write the records to a file
  with open('export/export-csv.json', 'w') as outfile:
    json.dump(covid_records, outfile)

  # Create the index
  client = SearchClient.create('FMXYI0LKWR', '994555c727b589b326344e574c0a959b')
  index = client.init_index('covid-geo')
  settings = index.get_settings()
  with open('export/index-settings.json', 'w') as outfile:
    json.dump(settings, outfile)
  index.clear_objects()
  index.save_objects(covid_records)

if __name__ == "__main__":
    main()