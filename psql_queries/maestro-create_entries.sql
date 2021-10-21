DROP TABLE IF EXISTS maestro;
CREATE TABLE maestro (
    audio_time float,
    y float,
    piece varchar(50) NOT NULL
);

\COPY maestro FROM '.\audio-maestro_piece0.csv'  DELIMITER ',' CSV HEADER;
