import concurrent.futures
import datetime as dt
import random
from time import time
from bazar import Bazar_parser as bazar
from file_manager_module import goFile
import requests
import sqlite3
import os
import psutil
import re


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

def ex_db_files():
    # Select a db file
    x = [file for file in [f for f in os.listdir() if f.endswith('.db')]]
    db_file = ""
    if not len(x):
        db_file = str(input("No db_files found\nCreate new: ")) + ".db"
        sqlite3.connect(db_file)
    elif len(x) == 1:
        db_file = x
    else:
        print(x)
        db = int(input("Choose db_file: "))
        db_file = x[db - 1]
    return db_file


def ex_txt_files():
    # Select a txt and load it to the memory
    b = [file for file in [f for f in os.listdir() if f.endswith('.txt')]]
    print("---------------")
    for y, w in enumerate(b):
        try:
            if "new_listing" not in w:
                file = goFile(w)
                lines = file.goCheck_lines()
                update = str(file.goLast_update())
                update = re.sub(r'(?=\.\d)\W\d*\s*', "", update)
                print("{} | {} |     Lines: {} |    Last Updated: {}".format(y, w, lines, update))
        except:
            pass
    txt = int(input("---------------" + "\n" + "Choose source txt_file: "))
    txt_file = b[txt]
    with open(txt_file, 'r') as wr:
        car_urls = [offer.replace("\n", "") for offer in wr]
        car_urls = car_urls[:-2]
    return car_urls


def cars_from_txt(filedatab, car):
    tabledatab = input(str("Input name of table: "))

    with sqlite3.connect(''.join(filedatab)) as dbconn:
        curs = dbconn.cursor()
        curs.execute("DROP TABLE IF EXISTS " + "".join(tabledatab))
        curs.execute("CREATE TABLE IF NOT EXISTS " + "".join(tabledatab) + "(Дата timestamp,"
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
        tstart1 = time()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            with requests.session() as sessiq:
                car_offer = {executor.submit(bazar.req, url, sessiq): url for url in
                             random.sample(car, len(car))}
                for details in concurrent.futures.as_completed(car_offer):
                    print(humanbytes(psutil.Process(os.getpid()).memory_info()[0]))
                    try:
                        data_bulk = bazar(details.result()[0], details.result()[1])
                        for values in data_bulk.car_details():
                            pass
                        for col in data_bulk.get_columns():
                            pass
                        exec_text = 'INSERT INTO ' + "".join(tabledatab) + '(' + ','.join(col) + ') VALUES(' + ','.join(
                            ['?'] * len(values)) + ')'
                        curs.execute(exec_text, values)
                        dbconn.commit()
                        # print(exec_text, values)
                        data_bulk.parser.decompose()
                    except Exception as err:
                        print(err)
    tend1 = round(time() - tstart1)
    print(str(dt.timedelta(seconds=tend1)))


db = ex_db_files()
txt = ex_txt_files()
cars_from_txt(db, txt)



# for x in db.tables_check():
#    print(x)

# with sqlite3.connect(db) as dbconn:
#     curs = dbconn.cursor()
#     # curs.execute("""SELECT COUNT(*) FROM (SELECT "rowid",* FROM """ + "".join(table) + """ ORDER BY "rowid" ASC);""")
#
#     query = "SELECT Марка_модел,Двигател, " \
#             "COUNT(*) Обяви FROM " + "".join(table) + """
#             GROUP BY Марка_модел,Двигател Having Обяви > 1 ORDER by "Обяви" DESC;"""
#     dbconn.commit()
#     df = pd.read_sql(query, dbconn)
#     print(df)
# print(db)
# print(txt)
