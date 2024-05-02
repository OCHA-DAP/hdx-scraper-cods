### Collector for COD Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-cods/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-cods/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-cods/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-cods?branch=main)

This script collects COD metadata from a manually maintained google sheet and updates corresponding datasets and resources on HDX. It then reads all COD metadata from HDX and overwrites the original google sheet. The scraper takes a few minutes to run. It makes one read/write to the metadata sheet and 1000 read/writes (API calls) to HDX in total. It does not create temporary files as it puts urls into HDX. It is run weekly. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-cods** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE