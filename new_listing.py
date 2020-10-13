import requests
from time import time
import random
import datetime as dt
import concurrent.futures
from bazar import Bazar_parser as bazar
from bs4 import BeautifulSoup as bs
from file_manager_module import goFile, Writer
import os
import re


def ex_src_file():
    # Select a txt and load it to the memory
    b = [file for file in [f for f in os.listdir() if f.endswith('.txt')]]
    print("---------------")
    for y, w in enumerate(b):
        try:
            if "new_listing" not in w:
                file = goFile(w)
                lines = file.goCheck_lines()
                update = str(file.goLast_update())
                update = re.sub(r"(?=\.\d)\W\d*\s*", "", update)
                print("{} | {} |     Lines: {} |    Last Updated: {}".format(y, w, lines, update))
        except:
            pass
            # print("Failed" + " | " + w)


ex_src_file()


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
            print("Generating Links: \n-------------")

            url_list = [link + "".join(str(x)) for x in range(1, pages + 1, 1)]
            global model
            model = make
            return url_list
        else:
            print("Make not available or doesn't exist")
    except Exception as err:
        print(err)


def new_list(mod, pag):
    # File should exists, checks the old records and put then in a list
    with open("".join(mod) + "_bazar.txt", 'r') as txt:
        car_urls = [offer.replace("\n", "") for offer in txt]
        car_urls = car_urls[:-2]

    # Start Threading and use the same session
    tstart = time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        with requests.session() as sessiq:
            car_offer = {executor.submit(bazar.req, url, sessiq): url for url in random.sample(pag, len(pag))}

            # Create new file for the new listings
            file = "new_listing_" + "".join(mod) + "_bazar.txt"
            checker = goFile(file)
            with Writer(file, 'w') as wr:
                for x in concurrent.futures.as_completed(car_offer):
                    cars = bazar(x.result()[0], x.result()[1])

                # Loop through the page listing and compare with the file
                # If new offer, write to the list
                    for new in cars.car_offers():
                        if new not in [f for f in car_urls]:
                            wr.write(new + "\n")
                            print("New Listing: {}".format(new))

        if os.path.getsize(file) == 0:
            print("-------------\nNo new listing")
            os.remove(file)
        else:
            # tm = "-------------\n{}".format(dt.datetime.now().strftime("%Y - %b - %d | %A %H:%M:%S"))
            # Calculate completed time
            checker.goRemove_dup_lines()
            # with open(file, 'a') as endline:
            #     endline.write(tm)
            print("File: {} created ".format(file))
            print("Total offers: {}".format(checker.goCheck_lines() - 2))
            tend = round(time() - tstart)
            print("Completed for: {}".format(str(dt.timedelta(seconds=tend))))
            # print(tm)
    print("-------------")


model = input("Input Model: ")
pages = search_page(model)
new_list(model, pages)
