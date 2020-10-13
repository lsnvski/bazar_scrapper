SELECT Count(Дата) Обяви,Линк,round(Cast(julianday('now') - julianday(Дата) as int))Изминали_дни,Марка_модел,Година,Двигател,Кутия,Тип,ROUND(AVG(Цена)) Средна_цена,Коментар
	From 
		test_audi
	WHERE 
		Волан IS NULL
		and
		Изминали_дни > 15
	GROUP by
		 Марка_модел
	Order by
		Обяви Desc