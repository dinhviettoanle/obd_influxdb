-- Date,TemperatureMin,TemperatureMax,TemperatureMoyenne,Precipitations,HygrometrieMin,HygrometrieMax,HygrometrieMoyenne

DROP TABLE IF EXISTS meteo_hist;
CREATE TABLE meteo_hist (
    TemperatureMin float,
    TemperatureMax float,
    TemperatureMoyenne float,
    Precipitations float,
    HygrometrieMin float,
    HygrometrieMax float,
    HygrometrieMoyenne float,
    Station varchar(50) NOT NULL,
    Heure timestamptz
);

\COPY meteo_hist FROM '.\meteo_hist-grenoble.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo_hist FROM '.\meteo_hist-lille.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo_hist FROM '.\meteo_hist-marseille.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo_hist FROM '.\meteo_hist-paris.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo_hist FROM '.\meteo_hist-toulouse.csv'  DELIMITER ',' CSV HEADER;

