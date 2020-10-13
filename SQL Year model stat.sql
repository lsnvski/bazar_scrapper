SELECT Линк,Марка_модел,Година,Двигател,Тип,COUNT(*) Обяви,MAX(Цена) Най_висока,ROUND(AVG(Цена)) Средна_цена,MIN(Цена) Най_ниска,
ROUND(AVG(Цена) / MIN(Цена),2) Процент,Коментар
	From 
		BMW
	WHERE 
		Волан IS NULL
	Group by 
		Марка_модел,Година,Двигател,Тип
	Having 
		Обяви > 1
	Order BY
		Обяви DESC
