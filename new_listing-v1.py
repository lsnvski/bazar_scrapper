from file_manager_module import goDatabase
import re
import sqlite3
import concurrent.futures
import requests
import random
import time
import os
from bs4 import BeautifulSoup as bs
from bazar import Bazar_parser as bazar


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

    with open("sitemap.txt", "r") as reader:
        cars = [offer.replace("\n", "") for offer in reader]
    try:
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
            # urls = [x[0] for x in curs.fetchall()]
            for link in curs.fetchall():
                dbconn.commit()
                yield link[0]
            # return urls
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
                            car_details.parser.decompose()
                except Exception as err:
                    print(err)
                    return None


def insert_db(urllist, db_tab, sess):
    import psutil
    print('-------------\nStart DB insert\n-------------')
    print(f"Total New Listing: {len(urllist)}")
    time.sleep(2)
    db_tab = db_tab.replace("-", "_").title()
    with concurrent.futures.ThreadPoolExecutor() as excecutor:
        parse_car = {excecutor.submit(bazar.req, url, sess): url for url in random.sample(urllist, len(urllist))}
        for num, car_details in enumerate(concurrent.futures.as_completed(parse_car)):
            print(humanbytes(psutil.Process(os.getpid()).memory_info()[0]))
            try:
                parsed_cars = bazar(car_details.result()[0], car_details.result()[1])
                for values in parsed_cars.car_details():
                    pass
                for col in parsed_cars.get_columns():
                    pass
                exec_text = 'INSERT INTO ' + "".join(db_tab) + '(' + ','.join(col) + ') VALUES(' + ','.join(
                    ['?'] * len(values)) + ')'
                curs.execute(exec_text, values)
                print("-------------\n", num, "Left: {}".format(len(urllist) - num))
                dbconn.commit()
            except Exception as err:
                print(err)
        checker.rem_chunk(db_tab)
        dbconn.commit()


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


if __name__ == '__main__':

    datab_file = ex_db_files()
    page_and_table = search_page()

    if page_and_table is not None:
        db_url_list = page_and_table[0]
        table = page_and_table[1]

        web_url_list = []
        for link in table_list(table, datab_file):
            web_url_list.append(link)

        with sqlite3.connect(datab_file) as dbconn:
            curs = dbconn.cursor()
            car_list = []

            checker = goDatabase(datab_file)
            with requests.session() as session:
                if web_url_list is not None:
                    for car in check_compare(db_url_list, web_url_list, session):
                        car_list.append(car)

                insert_db(car_list, table, session)
