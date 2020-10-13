from bs4 import BeautifulSoup as bs
from time import sleep
import random
from urllib.parse import urlparse
import re
import datetime
import locale


class Bazar_parser():
    def __init__(self, url, parser):
        self._url = url
        self._parser = parser

    @staticmethod
    def req(link, sess):
        r = sess.get(link)
        soup = bs(r.content, 'lxml')
        sleep(random.uniform(0.1, 0.3))
        return link, soup

    @staticmethod
    def check_redirect(link, sess):
        r = sess.get(link, allow_redirects=True)
        sleep(random.uniform(0.1, 0.3))
        return link, r.history, r.url

    # Extract all the car offers url's from search pages
    def car_offers(self):
        carers = []
        for x in self._parser.find('div', class_="row").find_all_next("a", href=True):
            if str(x["href"]).find("obiava-") != -1:
                try:
                    carers.append(x['href'])
                except Exception as err:
                    print(err)
        self._parser.decompose()
        return carers

    def car_details(self):
        try:
            date = self.get_update(),

            # Get link
            offer_url = str(self._url.replace("\n", "")),

            # Get Car ID
            car_id = re.search(r'(?<=-)\d*(?=/)', str(urlparse(self._url)[2])).group(0),

            data = tuple((x.find_next(attrs={"class": "span8"})).text for x in
                         self._parser.find_all('div', class_="row-fluid productInfo"))

            # Get Title
            # title = self._parser.find("title").text.split(" в ")[0],
            title = data[1],
            print(title)

            # Check if it's TOP offer
            if self._parser.find(class_="vip_inside") is not None:
                top = "Да",
            else:
                top = None,

            # Get Price
            price = re.sub(r'\w*(?=:).', "", self._parser.find('div', attrs={"itemprop": "offers"}).get_text(strip=True))
            if price[-1] == "\u20ac":
                price = int(re.sub(r'\D', "", price))*1.95,
            else:
                price = re.sub(r'\D', "", price),

            # Get Engine,Year,Mileage,etc
            data = data[2:]

            # Get steering wheel location
            wheel = str(self._parser.find_all('ul', class_="carOptions"))
            if "• Десен волан" in wheel:
                wheel = "Десен",
            else:
                wheel = None,

            # Get Comment if any:
            info = self._parser.find('div', attrs={"itemprop": "description"}).text.replace("\n", ""),
            if info is not None:
                pass
            else:
                info = None,

            yield date + car_id + offer_url + title + price + top + data + wheel + info

        except Exception as err:
            print(err)


    def get_columns(self):
        try:
            car_date = "Дата",
            car_url = "Линк",
            car_id1 = "ID",
            car_title = "Марка_модел",
            type_offer = "Top",
            car_price = "Цена",
            car_wheel = "Волан",
            col = [x.find_next(attrs={"class": "span4"}).get_text(strip=True) for x in
                   self._parser.find_all('div', class_="row-fluid productInfo")[2:]]
            for number, word in enumerate(col):
                if "Ск. кутия" in word:
                    col[number] = word.replace(". ", "_")
            col = tuple(col)
            comm = "Коментар",
            yield car_date + car_id1 + car_url + car_title + car_price + type_offer + col + car_wheel + comm

        except Exception as err:
            print(err)

    def get_update(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        locale.setlocale(locale.LC_ALL, '')
        try:
            date = self._parser.find(class_="adDate").get_text(strip=True)
            date = tuple(re.findall(r'((?<=ена\s)\w{3,})|((?<=\sна\s)\d*\s\w*)', date))
            for x in date:
                if x[0] == "":
                    month = datetime.datetime.strptime(
                        str(datetime.datetime.now().year) + " " + str((x[1].split(" ")[1].title())) + " " + str(x[1].split(" ")[0]), "%Y %B %d")
                    # print("{} | {}".format(self._url, month.strftime("%Y-%B-%d")))
                    return month.strftime("%Y-%m-%d")

                elif x[0] == "вчера":
                    # print("\n{} | {} | {} \n".format(self._url, yesterday.strftime("%Y-%B-%d"), "Вчера"))
                    return yesterday.strftime("%Y-%m-%d")

                elif x[0] == "днес":
                    # print("\n{} | {} | {} \n".format(datetime.datetime.today().strftime("%Y-%B-%d"), self._url, "Днес"))
                    return datetime.datetime.today().strftime("%Y-%m-%d")
                    # print(dict(zip(Bazar_parser.get_columns(self), Bazar_parser.car_details(self))))
                else:
                    pass

            self._parser.decompose()
        except Exception as err:
            print(err)
            print(self._url, "Sold")

    @property
    def url(self):
        return self._url

    @url.setter
    def set_url(self, value):
        self._url = value

    def __str__(self):
        return "{} | {}".format(str(self._url), self.parser)

    @property
    def parser(self):
        return self._parser






