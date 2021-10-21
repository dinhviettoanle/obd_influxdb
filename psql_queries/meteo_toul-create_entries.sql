DROP TABLE IF EXISTS meteo;
CREATE TABLE meteo (
    humidite float,
    pluie float,
    pluie_intensite_max float,
    force_moyenne_du_vecteur_vent float,
    direction_du_vecteur_vent_moyen float,
    direction_du_vecteur_vent_max float,
    force_rafale_max float,
    direction_du_vecteur_de_rafale_de_vent_max float,
    temperature float,
    pression float,
    heure timestamptz,
    station varchar(50) NOT NULL
);

\COPY meteo FROM '.\00-toulouse-valade.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo FROM '.\01-toulouse-meteopole.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo FROM '.\04-toulouse-ile-empalot.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo FROM '.\12-toulouse-montaudran.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo FROM '.\15-l-union-ecole.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo FROM '.\36-toulouse-purpan.csv'  DELIMITER ',' CSV HEADER;
\COPY meteo FROM '.\53-toulouse-ponsan.csv'  DELIMITER ',' CSV HEADER;

