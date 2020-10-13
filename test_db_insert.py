import requests
from bs4 import BeautifulSoup as bs
from bazar import Bazar_parser as bzp
import sys
import re
import urllib3
from file_manager_module import goFile
import os
import random

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

def column(c):
    car_date = "Дата",
    car_url = "Линк",
    car_id1 = "ID",
    car_title = "Марка_модел",
    type_offer = "Top",
    car_price = "Цена",
    car_wheel = "Волан",
    col = c
    for number, word in enumerate(col):
        if "Ск. кутия" in word:
            col[number] = word.replace(". ", "_")
    col = tuple(col)
    comm = "Коментар",
    yield car_date + car_id1 + car_url + car_title + car_price + type_offer + col + car_wheel + comm

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


test_cars = ex_txt_files()

with requests.session() as sess:

    for x in random.sample(test_cars, len(test_cars))[:20]:
        req = sess.get(x, stream=True)

        if req.encoding is None:
            req.encoding = 'utf-8'

        soup = []
        for link in req.iter_lines(decode_unicode=True):
            soup.append(link)
        soup = bs(str(soup),'lxml')
        print(soup)
    print(humanbytes(sys.getsizeof(soup)))