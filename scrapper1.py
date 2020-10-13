from bs4 import BeautifulSoup as bs
import requests
import sqlite3
from time import sleep
import random
from urllib.parse import urlparse
import re

url = "https://www.mobile.bg/pcgi/mobile.cgi?act=3&slink=f3sirz&f1="

html_raw = requests.get(url)
soup = bs(html_raw.content, "html.parser")


#get the total pages of the sarch criteria
def get_total_pages():
    for x in range(-4, -1):
        try:
            num = soup.find("span", attrs={"class": "pageNumbersInfo"}).get_text()[x:]
            print(num)
            print("Общо страници:", num)
            print("--------------------------------------")
            return int(num)
        except:
            pass


#convert the function to a int variable
total_pages = get_total_pages()

#create a list with all the urls
total_pages_list = []
for x in range(1, total_pages + 1, 1):
    total_pages_list.append(url + str(x))

print(total_pages_list)

#parse each page for URL's:
list_of_cars = []
for w,y in enumerate(random.sample(total_pages_list,len(total_pages_list))):
    soup = bs((requests.get(y)).content, "html.parser")
    sleep(random.uniform(0.1, 0.2))
    print(w,"Събиране на информация от", soup.find("span", class_="pageNumbersInfo").get_text())
    div = soup.find("td", {"rowspan": "2"})
    for z in div.findAll("a", href=True):
        list_of_cars.append(z["href"])
print("--------------------------------------")
urls_only = [x for x in list_of_cars if "act=4" in x]
urls_only = list(dict.fromkeys(urls_only))
urls_only = [s.strip('//') for s in urls_only]
urls_only = ["https://" + urls_only for urls_only in urls_only]
print("Намерени обяви:", len(urls_only))
print("--------------------------------------")

# Parse each url
def get_values(url,pars):

    # Get Sell offer ID, parse the url and substitude non digit characters
    car_id = urlparse(url)[4].split("=")[2]
    car_id = re.sub(r'\D','',car_id),

    # Get Title of sell offer
    title = pars.find("title").text.split(",")[0],

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

    # # Check for steering wheel location
    ang = str([extra_cat.find_next_siblings("div") for extra_cat in pars.find_all(class_="extra_cat") if "Интериор" in extra_cat])
    if "• Десен волан" in ang:
        ang =  "Десен",
    else:
        ang = None,

    # Search if there is a comment in the sell offer
    comment = tuple((x.find_next("table").get_text(strip=True)) for x in pars.find_all("div") if "Допълнителна информация:" in x)
    return car_id + href + title + price + offer + data + ang + comment
def define_column(pars):
    car_id = "ID",
    car_title = "Марка_модел",
    type_offer = "Top_Vip",
    car_url = "Линк",
    car_price = "Цена",
    car_wheel = "Волан",
    data = tuple((x.text.replace(" ","_")) for x in pars.find(class_="dilarData").find_all_next("li")[0::2])
    comm = tuple((x.text.replace(" " and ":", "_") for x in pars.find_all("div") if "Допълнителна информация:" in x))
    return car_id + car_url + car_title + car_price + type_offer + data + car_wheel + comm

def first_table(list):
    with sqlite3.connect('cars1.db') as conn:
        failed = []
        curs = conn.cursor()
        curs.execute("DROP TABLE IF EXISTS mobile")
        curs.execute("create table if not exists mobile("
                      "ID,Линк,Марка_модел,Цена,Top_Vip,Дата_на_производство,Тип_двигател,Мощност,Евростандарт,Скоростна_кутия,Категория,Пробег,Цвят,Волан,Допълнителна_информация)")
    for w,description in enumerate(random.sample(list,len(list))):
        try:
            desc = bs(requests.get(description).content, "html.parser")
            sleep(random.uniform(0.1, 0.2))
            col_names = define_column(desc)
            values = get_values(description, desc)
            exec_text = 'INSERT INTO mobile(' + ','.join(col_names) + ') VALUES(' + ','.join(['?'] * len(values)) + ')'
            curs.execute(exec_text, values)
            conn.commit()
            print("Обява", w, ":", values[2], " | Добавена, оставащи:", len(list) - w)
        except:
            print("Обява", w, ":", values[1:3], " | Неуспешно добавено")
            failed.append(values[1:3])
    if failed != None:

        print(failed)



first_table(urls_only)


