import concurrent.futures
import datetime as dt
import random
from bazar import Bazar_parser as bazar
from file_manager_module import goFile, goDatabase
import requests
import sqlite3
import os
import time
from bs4 import BeautifulSoup as bs
import re


def ex_db_files():
    # Select a db file
    x = [file for file in [f for f in os.listdir() if f.endswith('.db')]]
    if not len(x):
        file = str(input("No db_files found\nCreate new: ")) + ".db"
        sqlite3.connect(file)
        return file

    elif len(x) == 1:
        return x[0]

    else:
        print(x)
        db = int(input("Choose db_file: "))
        return str(x[db - 1])


def search_page():
    query = input("Input model: ")

    # Get all the models from sitemap
    with open("sitemap.txt", "r") as reader:
        cars = [offer.replace("\n", "") for offer in reader]

    try:
        # Check if input is in sitemap
        for make in tuple((x) for x in cars if query in x):
            link = ("https://bazar.bg/obiavi/avtomobili/" + "".join(
                make) + "?year_from=2000&year_to=2015&condition=2&pic=1&page=")
            req_get = requests.get(link)
            soup = bs(req_get.content, "lxml")
            pages = round(
                int(soup.find("div", class_="category").find_next('span').text.strip().replace("\xa0", "")) / 40)
            print("Total pages:", pages)
            print("Generating Links: \n-------------")
            table = re.sub(r'\W(?=\d)(?<=-)\d*', "", make)
            pages_list = [link + "".join(str(x)) for x in range(1, pages + 1, 1)]
            return pages_list, table.lower()
        else:
            print("Make not available or doesn't exist")
            return None
    except Exception as err:
        print(err)
        return None


def table_list(db_tab, db):
    db_tab = re.sub(r'\W', "_", db_tab).lower()

    # Choose table and check if it's valid
    with sqlite3.connect(db) as dbconn:
        dbprop = goDatabase(db)
        curs = dbconn.cursor()
        tables = (x[0].lower() for x in dbprop.tables_check())

        # Extract all the urls from the table
        # for tablica in tuple((x[0]).lower() for x in dbprop.tables_check() if table in x[0].lower()):
        if db_tab in (x for x in tables):
            curs.execute("Select Линк From " + "".join(db_tab))
            urls = [x[0] for x in curs.fetchall()]
            # for link in curs.fetchall():
            dbconn.commit()
            return urls
                # yield link[0]
        else:
            print(f"No such table {db_tab}")
            return None


def check_compare(pages, tablist, sessiq):
    if len(tablist) >= 1:
        # Extract all the new urls from the website
        with concurrent.futures.ThreadPoolExecutor() as executor:
            car_offer = {executor.submit(bazar.req, url, sessiq): url for url in random.sample(pages, len(pages))}
            for details in car_offer:
                car_details = bazar(details.result()[0], details.result()[1])

                # Check for urls that are not int hte Database's table URL Row
                # If new url is found, insert to table
                try:
                    for new in car_details.car_offers():
                        if new not in [f for f in tablist]:
                            print(f'New listing: {new}')
                            yield new
                except Exception as err:
                    print(err)
                    return None

def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string"""
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2)  # 1,048,576
    GB = float(KB ** 3)  # 1,073,741,824
    TB = float(KB ** 4)  # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B / TB)

import sys
db_file = ex_db_files()
pages_table = search_page()
table = pages_table[1]
pages = pages_table[0]

with requests.session() as sessiq:
    urls_from_table = table_list(table, db_file)

    for new in check_compare(pages, urls_from_table, sessiq):
        pass

    print("Entries of list: ", len(urls_from_table))
    print("Size of List: ", humanbytes(sys.getsizeof(urls_from_table)))

