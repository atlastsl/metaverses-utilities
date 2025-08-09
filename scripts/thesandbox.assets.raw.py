import argparse
import json
import os

import requests
from datetime import datetime
import time
import random

data = []
data_index = 0

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(FILE_DIR)


def get_land_info(x, y):
    url = f"https://api.sandbox.game/lands/coordinates?coordinateX={x}&coordinateY={y}&includeExperience=true&includeWallet=true&includeNft=true"
    response = requests.get(url)
    if response.status_code == 200 or response.status_code == 400:
        return response.json()
    else:
        return None


def get_land_info_with_retry(x, y, max_tries=5):
    tries = 0
    while tries < max_tries:
        resp = get_land_info(x, y)
        if resp is not None:
            if 'id' not in resp.keys():
                return None
            return resp
        tries += 1
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - #{tries} Try to get land ({x}, {y}) failed. Wait 5 "
              f"seconds to retry lands")
        time.sleep(5)
    return None


def get_experience(page):
    url = f"https://api.sandbox.game/lands/es/map-list/{page}?sortBy=new"
    response = requests.get(url)
    if response.status_code == 200 or response.status_code == 400:
        resp = response.json()
        if resp is not None and 'experiences' in resp.keys():
            return resp['experiences']
        return []
    else:
        return None


def get_experience_with_retry(page, max_tries=5):
    tries = 0
    while tries < max_tries:
        resp = get_experience(page)
        if resp is not None:
            return resp
        tries += 1
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - #{tries} Try to get experiences list page #{page} "
              f"failed. Wait 5 seconds to retry lands")
        time.sleep(5)
    return None


def save_data(data_chunk, data_name, buffer_size, force=False):
    global data, data_index
    if data_chunk is not None:
        if isinstance(data_chunk, list):
            data.extend(data_chunk)
        else:
            data.append(data_chunk)
    if force or len(data) >= buffer_size:
        data_index = data_index + 1
        with open(f"{ROOT_DIR}/files/thesandbox/{data_name}_{data_index}.json", "w") as f:
            json.dump(data, f, indent=2)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Saved {len(data)} {data_name}")
        data = []


def get_lands(buffer_size, test: bool = False):
    X_range = range(-204, 203)
    Y_range = range(-204, 203)
    if test:
        X_range = random.choices(X_range, k=5)
        Y_range = random.choices(Y_range, k=5)
    total = len(X_range) * len(Y_range)
    i = 0
    for x in X_range:
        for y in Y_range:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Processing {x}, {y}")
            land_info = get_land_info_with_retry(x, y, max_tries=5)
            save_data(land_info, 'lands_raw', buffer_size)
            i += 1
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Done {x}, {y} - {i}/{total}")
            time.sleep(0.15)
    save_data(None, 'lands_raw', buffer_size, True)


def get_experiences(buffer_size, test: bool = False):
    pages = range(1, 84)
    if test:
        pages = random.choices(pages, k=2)
    total = len(pages)
    i = 0
    for page in pages:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Processing #{page}")
        experiences_info = get_experience_with_retry(page, max_tries=2)
        save_data(experiences_info, 'experiences_raw', buffer_size)
        i += 1
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Done #{page} - {i}/{total}")
        time.sleep(0.15)
    save_data(None, 'experiences_raw', buffer_size, True)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--purpose", action="store", type=str,
                        choices=['lands', 'experiences'], required=True)
    parser.add_argument("-b", "--buffer-size", default=1, action="store", type=int,
                        help="Buffer size for save in JSON file")
    parser.add_argument("-t", "--test", default=False, type=bool, action="store",
                        help="Is for testing ?")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if args.purpose == 'lands':
        get_lands(args.buffer_size, test=args.test)
    elif args.purpose == 'experiences':
        get_experiences(args.buffer_size, test=args.test)
    else:
        print("Invalid purpose")


if __name__ == '__main__':
    main()
