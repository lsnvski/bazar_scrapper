import concurrent.futures
import datetime as dt
import random
from time import time
from bazar import Bazar_parser as bazar
import requests
from bs4 import BeautifulSoup as bs
from file_manager_module import goFile, Writer

def search_page(query):
    with open("sitemap.txt", "r") as reader:
        cars = [offer.replace("\n", "") for offer in reader]
    try:
        for make in tuple((x) for x in cars if query in x):
            link = ("https://bazar.bg/obiavi/avtomobili/" + "".join(
                make) + "?year_from=2000&year_to=2015&condition=2&pic=1&page=")
            soup = bs(requests.get(link).content, "html.parser")
            pages = round(
                int(soup.find("div", class_="category").find_next('span').text.strip().replace("\xa0", "")) / 40)
            print("Total pages:", pages)
            print("Generating Links: ")
            url_list = [link + "".join(str(x)) for x in range(1, pages + 1, 1)]
            global model
            model = make
            return url_list
        else:
            print("Make not available or doesn't exist")
    except Exception as err:
        print(err)


model = input("Input Model: ")


def first_list(mod, urll):
    tstart = time()
    file = "".join(mod) + "_bazar.txt"
    checker = goFile(file)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        with requests.session() as session:
            thread_for_ulr = {executor.submit(bazar.req, url, session): url for url in
                              random.sample(urll, len(urll))}
            with Writer(file, "w") as writer:
                for scrapped_cars in concurrent.futures.as_completed(thread_for_ulr):
                    try:
                        urlsd = scrapped_cars.result()[0]
                        bs4element = scrapped_cars.result()[1]
                        cars = bazar(urlsd, bs4element).car_offers()
                        for x in cars:
                            print(x)
                            writer.write(str(x) + "\n")
                    except Exception as err:
                        print(err)
                        break
    checker.goRemove_dup_lines()
    print("File: {} created ".format(file))
    print("Total offers: {}".format(checker.goCheck_lines()))
    tend = round(time() - tstart)
    print("Completed for: {}".format(str(dt.timedelta(seconds=tend))))
    # print(tm)


content = search_page(model)


url_list = first_list(model, content)
