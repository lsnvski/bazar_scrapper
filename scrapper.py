from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import sqlite3
from time import sleep
from time import time
import random
import datetime as dt
from urllib.parse import urlparse
import re
from scrapper1 import urls_only



def get_values(url,pars):

    # Get Sell offer ID, parse the url and substitude non digit characters
    car_id = urlparse(url)[4].split("=")[2]
    car_id = re.sub(r'\D','',car_id),

    # Get Title of sell offer
    title = pars.find("title").text.split(",")[0],
    print(title)

    # Get URL of sell offer
    href = url,

    # Check for TOP or VIP offer
    offer = ()
    if pars.find(class_="img VIP") != None:
        offer = "Vip",
    elif pars.find(class_="img TOP") != None:
        offer = "Top",
    else:
        offer = None,
        pass

    # Get Price of the car
    # Check currency and convert to BGN if EUR
    price = pars.find(id="details_price").text
    if "".join(str(x) for x in price.split()[-1:]) == "EUR":
        try:
            price = int(int("".join(str(x) for x in price.split()[:-1])) * 1.95),
        except:
            pass
    else:
        price = re.sub(r'\D', '', price),

    # Get details of the car
    # Iterate over the list, return TRUE for elements with digits
    # Remove all the non digits charts and return only numbers
    # Find the position of the elements so they can be replaced
    data = [x.text for x in pars.find(class_="dilarData").find_all_next("li")[1::2]]
    for w, i in enumerate(data):
        if bool(re.search(r'\d',i)) == True:
            data[w] = re.sub(r'\D','',i)
    data = tuple(data)

    # Search if there is a comment in the sell offer
    comment = tuple((x.find_next("table").get_text(strip=True)) for x in pars.find_all("div") if "Допълнителна информация:" in x)
    return car_id + href + title + price + offer + data + comment

def define_column(pars):
    car_id = "ID",
    car_title = "Марка_модел",
    type_offer = "Top_Vip",
    car_url = "Линк",
    car_price = "Цена",
    data = tuple((x.text.replace(" ","_")) for x in pars.find(class_="dilarData").find_all_next("li")[0::2])
    comm = tuple((x.text.replace(" ","_").replace(":","")) for x in pars.find_all("div") if "Допълнителна информация:" in x)
    return car_id + car_url + car_title + car_price + type_offer + data + comm



def first_table(list):
    with sqlite3.connect('cars1.db') as conn:
        curs = conn.cursor()
        curs.execute("DROP TABLE IF EXISTS cars")
    for description in random.sample(list,len(list)):
        desc = bs(requests.get(description).content, "html.parser")
        sleep(random.uniform(0.1, 0.2))
        curs.execute("create table if not exists cars("
                      "ID int,Линк,Марка_модел,Цена,Top_Vip,Дата_на_производство,Тип_двигател,Мощност,Евростандарт,Скоростна_кутия,Категория,Пробег,Цвят,Допълнителна_информация)")
        col_names = define_column(desc)
        values = get_values(description,desc)
        exec_text = 'INSERT INTO cars(' + ','.join(col_names) + ') VALUES(' + ','.join(['?'] * len(values)) + ')'
        print(exec_text)
        curs.execute(exec_text, values)
        conn.commit()

first_table(urls_only)





