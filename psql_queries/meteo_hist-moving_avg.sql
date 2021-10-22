SELECT
    m.station, m.heure,
    COALESCE(AVG(m.temperaturemoyenne) OVER
        (PARTITION BY station ORDER BY m.heure ROWS BETWEEN 83 PRECEDING AND CURRENT ROW), m.temperaturemoyenne) as rollingAvg
FROM meteo_hist AS m
ORDER BY station;