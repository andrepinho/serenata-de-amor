from geopy.exc import GeocoderQuotaExceeded, GeocoderTimedOut
from geopy.geocoders import GoogleV3
import pickle
import os
import pandas as pd
import re

DATASET_PATH = 'data/companies.xz'
TEMP_PATH = 'data/companies'

def write_company_geocoding(company_location, cnpj):
    print('Writing %s' % cnpj)
    with open('%s/%s.pkl' % (TEMP_PATH, cnpj), 'wb') as f:
        pickle.dump(company_location, f, pickle.HIGHEST_PROTOCOL)

def fetch_company_coordinates(company):
    address = ' '.join(company[['address', 'number', 'zip_code', 'neighborhood', 'city', 'state']].dropna())
    if address == '':
        print('No address information for', company['name'], company['cnpj'])
        return pd.Series()
    else:
        print(address)
        try:
            location = geolocator.geocode(address)
        except GeocoderTimedOut as e:
            print('Timeout with %s' % address)
            return pd.Series()

        if location:
            cnpj = re.sub(r'[./-]', "", company['cnpj'])
            write_company_geocoding(location, cnpj)
            return pd.Series({'latitude': location.latitude,
                              'longitude': location.longitude})
        else:
            print('No coordinates found for', company['name'], company['cnpj'], address)
            return pd.Series()

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
data = pd.concat([remaining_companies, remaining_companies.apply(fetch_company_coordinates, axis=1)], axis=1)

# data.to_csv(DATASET_PATH,
#             compression='xz',
#             encoding='utf-8',
#             index=False)
