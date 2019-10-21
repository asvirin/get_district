import requests
import pandas as pd
import redis
import json
import ast
import os

API_KEY = os.environ['API_KEY']

def get_street_district(geocode):
    params = {"apikey": API_KEY,
               "geocode": geocode,
               "format": "json"}

    r = requests.get('https://geocode-maps.yandex.ru/1.x/', params=params)
    
    try:
        street = r.json()['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty']['GeocoderMetaData']['AddressDetails']['Country']['AdministrativeArea']['Locality']['Thoroughfare']['ThoroughfareName']
    except:
        street = "Не определил"
        
    try:   
        district = r.json()['response']['GeoObjectCollection']['featureMember'][2]['GeoObject']['metaDataProperty']['GeocoderMetaData']['Address']['Components'][-1]['name']
    except:
        district = "Не определил"
        
    return street, district 

def get_coordinate(address):
    geocode = address.encode('UTF-8')
    params = {"apikey": API_KEY,
               "geocode": geocode,
               "format": "json"}

    r = requests.get('https://geocode-maps.yandex.ru/1.x/', params=params)
    
    try:
        geocode = (r.json()['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']).replace(' ', ',')
    except:
        geocode = '00.000000,00.000000'
        
    return get_street_district(geocode)
        

def get_dict_with_address_district(list_address):
    for address in list_address:
        try:
            if r.get(address).decode('utf8'):
                continue
        except:
            street, district = get_coordinate(address)
            values = {'district': district, 'street': street}
            r.set(address, str(values))
            
if __name__ == "__main__":
    redis_host = os.environ['REDIS_HOST']
    redis_port = os.environ['REDIS_PORT']
    redis_password = os.environ['REDIS_PASSWORD']
    redis_db = os.environ['REDIS_DB']
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, db=redis_db)
    
    path_to_file = os.environ['PATH_TO_FILE']
    name_of_column = os.environ['NAME_OF_COLUMN']
    
    data = pd.read_csv(path_to_file)
    list_unique_address = data[name_of_column].unique().tolist()
    
    get_dict_with_address_district(list_unique_address)
