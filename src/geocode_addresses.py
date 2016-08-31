from concurrent import futures
from geopy.exc import GeocoderQuotaExceeded, GeocoderTimedOut
from geopy.geocoders import GoogleV3
import pickle
import os
import pandas as pd
import re

DATASET_PATH = 'data/companies.xz'
TEMP_PATH = 'data/companies'

def write_company_geocoding(company_location, cnpj):
    cnpj = re.sub(r'[./-]', "", cnpj)
    print('Writing %s' % cnpj)
    with open('%s/%s.pkl' % (TEMP_PATH, cnpj), 'wb') as f:
        pickle.dump(company_location, f, pickle.HIGHEST_PROTOCOL)

def fetch_company_coordinates(company):
    address = ' '.join(company[['address', 'number', 'zip_code', 'neighborhood', 'city', 'state']].dropna())
    if address == '':
        print('No address information for', company['name'], company['cnpj'])
        return None
    else:
        location = geolocator.geocode(address)
        return location



if not os.path.exists(TEMP_PATH):
    os.makedirs(TEMP_PATH)

geolocator = GoogleV3()
data = pd.read_csv(DATASET_PATH, low_memory=False)

geocoding_already_fetched = [filename[:14]
                   for filename in os.listdir(TEMP_PATH)
                   if filename.endswith('.pkl')]
remaining_companies = data[~data['cnpj'].str.replace(r'[./-]', ''). \
    isin(geocoding_already_fetched)]

print(len(data), len(remaining_companies))

with futures.ThreadPoolExecutor(max_workers=20) as executor:
    future_to_geocoding = dict()
    for index, company in remaining_companies.iterrows():
        future = executor.submit(fetch_company_coordinates, company)
        future_to_geocoding[future] = company

    for future in futures.as_completed(future_to_geocoding):
        company = future_to_geocoding[future]
        if future.exception() is not None:
            print('%r raised an exception: %s' % (company['cnpj'], future.exception()))
        elif future.result() is not None:
            print('saving')
            write_company_geocoding(future.result(), company['cnpj'])

data = pd.concat([remaining_companies, remaining_companies.apply(fetch_company_coordinates, axis=1)], axis=1)

# data.to_csv(DATASET_PATH,
#             compression='xz',
#             encoding='utf-8',
#             index=False)
