Select Марка_модел,Двигател,Година,Цена,count(*) Обяви,Top,Волан,Коментар 
From BMW_sold
Group by Марка_модел,Двигател,Година
Order by Обяви DESC