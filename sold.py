from file_manager_module import goDatabase
from bazar import Bazar_parser
import os
import sqlite3
import concurrent.futures
import requests
import random
import time
import datetime as dt
from bs4 import  BeautifulSoup as bs


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


def check_sold(db):

    # Choose table and check if it's valid
    with sqlite3.connect(''.join(db)) as dbconn:
        dbprop = goDatabase(''.join(db))
        curs = dbconn.cursor()
        tables = [x[0] for x in dbprop.tables_check()]
        print(tables)
        table = str(input("Choose table: "))
        if table in tables:
            sold_table = str(table) + "_" + "".join("Sold")
            # curs.execute("DROP TABLE IF EXISTS " + "".join(str(table)) + "_" + "".join("Sold"))
            curs.execute("CREATE TABLE IF NOT EXISTS " + "".join(sold_table) +
                         "(Дата timestamp,"
                         "ID int,"
                         "Линк,"
                         "Марка_модел,"
                         "Цена int,"
                         "Top,"
                         "Двигател,"
                         "Ск_кутия,"
                         "Тип,"
                         "Година int,"
                         "Пробег,"
                         "Волан,"
                         "Коментар)")
            curs.execute("SELECT Линк FROM " + "".join(table))
            b = [x[0] for x in curs.fetchall()]
            dbconn.commit()
            print("Total offers found: {}".format(len(b)))
        else:
            print("No such table")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            with requests.session() as sessiq:

                updated = {executor.submit(Bazar_parser.check_redirect, url, sessiq): url for url in random.sample(b, len(b))}
                tstart = time.time()
                for num, details in enumerate(concurrent.futures.as_completed(updated)):
                    print(num, details.result()[0], "Left: {}".format(len(b) - num))

                    # Check history of redirects
                    if details.result()[1]:

                        if "obiava" not in details.result()[2]:
                            curs.execute("INSERT INTO " + "".join(sold_table) +
                                         " Select * FROM " + "".join(table) +
                                         " WHERE Линк like " + "".join("'%") + "".join(details.result()[0]) + "".join("%';"))
                            curs.execute("DELETE FROM " + "".join(table) + " WHERE Линк like " + "".join("'%") + "".join(details.result()[0]) + "".join("%';"))
                            dbconn.commit()
                            print("\nSold\n")

                        else:
                            pars = bs(sessiq.get(details.result()[0]).content, 'lxml')
                            da = Bazar_parser(details.result()[0], pars)
                            for x in da.car_details():
                                print("\n", x, "\n")
                                break

                    if num == 100:
                        elapsed = round(time.time() - tstart)
                        print("Elapsed time: {}".format(dt.timedelta(seconds=elapsed)))
                        remaining = round((elapsed / num) * len(b) - num)
                        print("Time remainig: {}".format(dt.timedelta(seconds=remaining)))
                        time.sleep(5)


check_sold(ex_db_files())
