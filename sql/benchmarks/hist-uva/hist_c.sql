SELECT * FROM
	(SELECT h2.name, histoCompare(h1.histoData, h2.histoData, 2, 256) compare
	FROM histogram_tab h1, histogram_tab h2
	WHERE (h1.name='SummrFun/732023.JPG') AND (h1.setId = 3) AND (h2.setId = 3)
	ORDER BY histoCompare(h1.histoData, h2.histoData, 2, 256))
WHERE rownum < 10;
