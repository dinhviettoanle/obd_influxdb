SELECT m.temperature, m.heure, 
AVG(m.temperature) OVER(ORDER BY m.heure ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_temp
FROM meteo AS m
WHERE m.station='12-toulouse-montaudran';