CREATE TABLE necesidades_nna
(
    id integer PRIMARY KEY,
	id_mesa integer,
    dominant_topic integer,
    topic_perc_contrib float,
    keywords varchar(256),
    text varchar(1024),
    topic varchar(256),
    organizacion varchar(128),
    rango_edades_id integer,
    cantidad_participantes integer,
    inst varchar(64),
    topico varchar(32),
    region_iso varchar(16),
    region_name varchar(32),
    comuna_name varchar(32),
    source varchar(16), 
    target varchar(16)
);
