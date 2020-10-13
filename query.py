import sqlite3
import os
from file_manager_module import goDatabase
from pandas import DataFrame as dt
import pandas
from pivottablejs import pivot_ui
import openpyxl


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

db_file = ex_db_files()
model = str(input("Model: ")).title()
checker = goDatabase(db_file)


def query(obj):
    # query = "SELECT " \
    #         "     Линк,Марка_модел,Година,Двигател,Тип," \
    #         "COUNT(*) Обяви," \
    #         "MAX(Цена) Най_висока," \
    #         "ROUND(AVG(Цена)) Средна_цена," \
    #         "MIN(Цена) Най_ниска," \
    #         "ROUND(AVG(Цена) / MIN(Цена),2) Процент,Коментар " \
    #         "From " + "".join(obj) + \
    #         " WHERE Волан IS NULL " \
    #         "Group by " \
    #         "       Марка_модел,Година,Двигател,Тип " \
    #         "Having Процент < 4 " \
    #         "       and " \
    #         "       Обяви > 1 " \
    #         "Order BY Процент Desc"

    query = "SELECT Марка_модел,Година,Тип,Двигател,Цена FROM " + "".join(obj)

    return query


with sqlite3.connect(db_file) as dbconn:
    from openpyxl import load_workbook
    cursor = dbconn.cursor()
    for x in checker.tables_check():
        if model in x:
            cursor.execute(query(x[0]))

            column = [x[0] for x in cursor.description]
            row = cursor.fetchall()
            # newData = [tuple(str(x[1]).replace("Mercedes-Benz", "") for x in (d for d in row))]
            newData = [tuple(str(y).replace("Mercedes-Benz", "")for y in tab) for tab in row]

            df1 = dt(newData, columns=column)
            print(df1)
            # pivot_ui(df1)
            with pandas.ExcelWriter("Mercedes_Benz_Graph.xlsx", mode="a") as writer:
                print(df1.to_excel(writer, sheet_name="Sheet1", startrow=0, startcol=0))
            break

        elif model == "All":
            all = query(x[0])
            cursor.execute(all)
            row = [x[0] for x in cursor.description]
            column = cursor.fetchall()
            df = pandas.DataFrame.to_excel(column, columns=row)
            print(x[0], df, "\n")

        else:
            print(model)
            pass








