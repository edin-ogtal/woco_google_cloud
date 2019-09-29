# Collecting and managing data using Google App Engine, Cloud Storage and BigQuery 

This repo contains a simple app, which downloads data from an API and stores it. The purpose of this repo is to provide a realistic example to illustrate how all of this can be automated using Google App Engine. 

The data stems from the [open data public api](https://portal.opendata.dk/dataset/realtids-trafikdata
) and consists of realtime trafic measurements from Aarhus. The data is downloaded every 5 minutes and stored in a series of buckets. One master bucket containing all of the data, and a series of smaller buckets containing subsets of the data. 
The data is finally saved in a Google BigQuery table.

to run: 
`export GOOGLE_APPLICATION_CREDENTIALS='yourappkey.json'`

`gcloud app create`

`gcloud app deploy`

snippet:
`pipenv run pip freeze > requirements.txt`
