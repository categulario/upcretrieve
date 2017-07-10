#!/usr/bin/env python3
import csv
from pprint import pprint
from itertools import zip_longest
import requests
from time import sleep
from datetime import datetime
import pickle
import os

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

    if os.path.isfile('./checked.data'):
        checked = pickle.load(open('./checked.data', 'rb'))
    else:
        checked = set()

    for upc, values in data.items():
        if upc in checked:
            print("skip {}".format(upc))
            continue

        r = requests.get('https://api.upcitemdb.com/prod/trial/lookup', params={
            'upc' : upc,
        })

        if r.status_code == 200:
            items = r.json()['items']
            if len(items) == 0:
                # TODO disable this code
                print("no product found for {}".format(upc))
            else:
                values['Item Name'] = items[0]['title']

            checked.add(upc)
            pickle.dump(checked, open('./checked.data', 'wb'))

            sleep(1)
        elif r.status_code == 429:
            code = r.json()['code']

            print(code)

            reset = datetime.fromtimestamp(int(r.headers['X-RateLimit-Reset']))
            delta = reset - datetime.now()
            print("time to sleep for {} seconds".format(delta.seconds))
            sleep(delta.seconds)
        else:
            print(r.text)
            break

    print('{} products found in upcitemdb.com'.format(count))

    return data

if __name__ == '__main__':
    data = query_upcitemdb(extract_uniques('./data/inventario.csv'))

    with open('./result.csv', 'w') as resultfile:
        fieldnames = next(iter(data.items()))[1].keys()
        writer = csv.DictWriter(resultfile, fieldnames=fieldnames)

        writer.writeheader()

        for upc, values in data.items():
            writer.writerow(values)
