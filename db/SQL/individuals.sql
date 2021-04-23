CREATE TABLE individuals
(
	id varchar(64) PRIMARY KEY,
	date date,
	age integer,
    comuna_id integer REFERENCES comunas(id),
	level varchar(64),
	age_range varchar(5),
	online Boolean
);