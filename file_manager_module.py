import os
from collections import Counter
import sqlite3
import datetime as dt
import pandas as pd


class Writer():

    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode

    def __enter__(self):
        self.file = open(self.filename, self.mode)
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.file.write("-------------\n{}".format(dt.datetime.now().strftime("%Y - %b - %d | %A %H:%M:%S")))
        finally:
            self.file.close()


class goFile:

    def __init__(self, name_file):
        self._name = str(name_file)

    def goLast_update(self):
        if self.goCheck_lines():
            num = int(self.goCheck_lines() - 1)
            with open(self._name, "r") as reader:
                for i, line in enumerate(reader):
                    if i == num:
                        return (dt.datetime.now() - dt.datetime.strptime(line, "%Y - %b - %d | %A %H:%M:%S"))
        else:
            return None

    def goCheck_lines(self):
        if os.path.isfile(self._name) and \
                os.path.getsize(self._name):
            with open(self._name) as file_open:
                for number, urls in enumerate(file_open.readlines()):
                    pass
            return int(number + 1)
        else:
            # print("File is empty")
            return None

    def goRemove_dup_lines(self):

        # Check if file is empty
        if not goFile.goCheck_lines(self):
            return None
        else:
            # Raise the counter and create list
            cnt = Counter()
            offers = []

            # Use the counter to check how many times an offer is duplicated
            with open(self._name, "r") as txt:
                for offer in txt:
                    cnt[offer] += 1
                    offers.append(offer)

            # Remove the duplicates from the file
            b = sum([x[1] - 1 for x in [d for d in cnt.items() if d[1] > 1]])
            if b != 0:
                offers = list(dict.fromkeys(offers))
                with open(self._name, "w") as txt_nodub:
                    for x in offers:
                        txt_nodub.writelines(x)
                print("-------------\nTotal duplicates removed: {}".format(b))
            else:
                print("-------------\nNo duplicates")
            offers.clear()
            cnt.clear()

    @property
    def new_name(self):
        b = [file for file in [f for f in os.listdir() if f.endswith('.txt')]]
        print("---------------")
        for y, w in enumerate(b):
            print("{} | {} | Lines: {}".format(str(y), w, str(goFile(w).goCheck_lines())))
        try:
            txt = int(input("---------------" + "\n" + "Choose source txt_file: "))
            self._name = b[txt]
        except Exception as err:
            print(err)
        return self._name

    @property
    def name(self):
        return self._name

    def __str__(self):
        return "{}".format(self._name)


# try:
#     x = [file for file in [f for f in os.listdir() if f.endswith('.txt')]]
#     print(x)
#     num = int(input("Choose a file number: "))
#     print(x[num - 1])
#     checker = goFile(x[num - 1])
#     print(checker.goRemove_dup_lines())
# except IndexError as err:
#     print("\nTotal files : {}".format(len(x)))
# except ValueError as err1:
#     print("Input must be integer")


class goDatabase():
    def __init__(self, database_name):
        self._database_name = str(database_name)

    def tables_check(self):
        with sqlite3.connect("".join(self._database_name)) as dbconn:
            curs = dbconn.cursor()
        curs.execute("""Select name FROM sqlite_master""")
        for x in curs.fetchall():
            yield x

    def rem_chunk(self, tabl):
        with sqlite3.connect(self._database_name) as conn:
            curs = conn.cursor()
            for tables in goDatabase.tables_check(self):
                try:
                    if tabl in tables or tabl == "all":

                        # Store the removed link in another table
                        a = curs.execute(
                            """Update {} 
                                Set 
                                    Цена = cast(Цена as int)""".format(tables[0])).rowcount
                        b = curs.execute(
                            """Update {} 
                                Set 
                                    Цена = Null 
                                WHERE 
                                    Цена = '' 
                                    or
                                    Цена = 1234
                                    or
                                    Цена = 1111""".format(tables[0])).rowcount
                        g = curs.execute(
                            """DELETE FROM {} 
                                WHERE 
                                    Линк like '%bronia%' 
                                    or 
                                    Линк Like '%chasti%'
                                    or
                                    Линк Like '%dzhanti%'""".format(tables[0])).rowcount
                        e = curs.execute(
                            """DELETE FROM {} 
                                WHERE 
                                    LENGTH(Цена) < 4""".format(tables[0])).rowcount
                        j = curs.execute(
                            """Delete From {} 
                                WHERE 
                                    Коментар like '%АВТОКОМПЛЕКС%'""".format(tables[0])).rowcount
                        z = curs.execute(
                            """Delete FROM {} 
                                WHERE 
                                    Коментар Like '%БЕЗЛИХВЕН СОБСТВЕН ЛИЗИНГ%'""".format(tables[0])).rowcount
                        i = curs.execute(
                            """DELETE from {} 
                                WHERE 
                                    ROWID not in 
                                    (SELECT min(ROWID) from {} group by ID)""".format(tables[0], tables[0])).rowcount

                        print(f"     {tables[0]}"
                              f"\n"
                              f"\nRemoved decimals: {a}"
                              f"\nEmpty Price : {b}"
                              f"\nFor Parts : {g}"
                              f"\nUnrealistic Price : {e}"
                              f"\nSalvage and Payments price : {j} , {z}"
                              f"\nDuplicated offers : {i}"
                              f"\n"
                              f"\nTotal Removed : {b + g + e + j + z + i}")
                        print("---------------")
                        conn.commit()
                except Exception as err:
                    print(err)

    def __str__(self):
        return "{}".format(self._database_name)
