#!/usr/bin/env python3
import csv
from pprint import pprint
from itertools import zip_longest
import requests

def update_info(dest, source, addone=True):
    for key in dest.keys():
        if key in source and source[key] and not dest[key]:
            dest[key] = source[key]

    if addone:
        dest['quantity1'] += 1

def fill_blanks(row):
    if not row['quantity1']:
        row['quantity1'] = 1
    elif type(row['quantity1']) == str:
        row['quantity1'] = int(row['quantity1'])

def chunks(iterable, size=20):
    "Collect data into fixed-length chunks or blocks"
    # chunks('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * size
    return zip_longest(*args)

def extract_uniques(filename):
    data = dict()
    count = 0

    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            barcode = row['UPC/EAN/ISBN']

            fill_blanks(row)

            already_checked = barcode in data

            if barcode and not already_checked:
                data[barcode] = row
            elif barcode and already_checked:
                update_info(data[barcode], row)
            else:
                pass # no barcode

            count += 1

    print('{} products checked, {} unique products'.format(count, len(data)))

    return data

def query_upcitemdb(data):
    count = 0

    for chunk in chunks(data, size=1):
        r = requests.get('https://api.upcitemdb.com/prod/trial/lookup', params={
            'upc' : ','.join(filter(lambda x: x, chunk)),
        })

        if r.status_code == 200:
            for item in r.json()['items']:
                print(item['title'])
        elif r.status_code == 429:
            code = r.json()['code']
            if code == 'TOO_FAST':
                print('TOO_FAST')
            elif code == 'EXCEED_LIMIT':
                print('EXCEED_LIMIT')

            print("X-RateLimit-Limit: {}".format(r.headers['X-RateLimit-Limit']))
            print("X-RateLimit-Reset: {}".format(r.headers['X-RateLimit-Reset']))
            print("X-RateLimit-Remaining: {}".format(r.headers['X-RateLimit-Remaining']))

    print('{} products found in upcitemdb.com'.format(count))

    return data

if __name__ == '__main__':
    data = query_upcitemdb(extract_uniques('./data/inventario.csv'))
