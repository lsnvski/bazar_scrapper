import requests
from time import time
import random
import datetime as dt
import concurrent.futures
from bazar import Bazar_parser as bazar
from file_manager_module import goFile
import os


def ex_files():

    # Load content of txt to memory

    x = [file for file in [f for f in os.listdir() if f.endswith('.txt')]]
    for y, w in enumerate(x):
        print("{} | {} | Lines: {}".format(str(y), w, str(goFile(w).goCheck_lines())))
    num = int(input("Choose a file number: "))
    file = x[num]
    print(file)
    with open(file, 'r') as wr:
        car_urls = [offer.replace("\n", "") for offer in wr]
        car_urls = car_urls[:-2]
    return car_urls


def update(file):
    tstart1 = time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        with requests.session() as sessiq:
            car_offer = {executor.submit(bazar.req, url, sessiq): url for url in
                         random.sample(file, len(file))}

            for upd in concurrent.futures.as_completed(car_offer):
                try:
                    cars = bazar(upd.result()[0], upd.result()[1])
                    cars.get_update()
                except Exception as err:
                    print(err)

    print(goFile(file).goCheck_lines())
    tend1 = round(time() - tstart1)
    print(str(dt.timedelta(seconds=tend1)))


b = ex_files()
start = update(b)
