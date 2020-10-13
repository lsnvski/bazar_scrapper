Update Mitsubishi
Set Дата = replace(Дата, substr(Дата, 0, 5), '2019')
Where Дата > date('now', 'localtime')