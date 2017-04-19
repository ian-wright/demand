#!/usr/bin/env

import requests
import json

my_key = '7eb8f2869a253fc97eeaefee4154550a'
url = 'http://api.eia.gov/category/?api_key=' + my_key + '&category_id=2122628'

data = requests.get(url).json()

with open('series_dir.json', 'w') as f:
    json.dump(data, f)