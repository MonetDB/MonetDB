SELECT name, dist
FROM(
SELECT h2.Name, SUM(1
	-LEAST(h1.Bin1, h2.Bin1)
) / COUNT(*) dist
FROM Histogram256_tab h1, Histogram256_tab h2, HistogramSet_tab s
WHERE (h1.setId = s.Id)
	AND (h2.setId = s.Id)
	AND (s.Name = 'CorelC')
	AND (h1.Name = 'Abstract/583007.JPG')
	AND (h1.HistoName =  h2.HistoName)
	AND ROWNUM < 10
GROUP BY h2.Name 
ORDER BY dist
)
WHERE ROWNUM < 10;

