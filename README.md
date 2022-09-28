# COVID-19 Case GeoSearch using Algolia 

This repo contains a simple React application for displaying COVID-19 case information on a world map. The front end pulls its data from an Algolia index.

The repo is meant to compliment the blog post [Building a COVID-19 geosearch index using CSV files, MongoDB, or GraphQL](https://www.algolia.com/blog/engineering/building-a-covid-19-geosearch-index-using-csv-files-mongodb-or-graphql/)

The interesting part is the `scripts` directory, which contains python scripts to demonstrate various ways of retrieving the John Hopkins University case information from various data sources:

- CSV files directly from the [JHU repo](https://github.com/CSSEGISandData/COVID-19)
- MongoDB database hosted on Atlas
- GraphQL interface to the MongoDB
- REST API

You can use these scripts as examples of various ways to retrieve data and save it to an Algolia index, then use the React front end to visualize the data (you will need an Algolia account).
