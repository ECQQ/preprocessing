CREATE TABLE individual
(
	id varchar(32) PRIMARY KEY,
	age integer,
	level varchar(64),
	age_range varchar(5),	
	comuna integer REFERENCES comuna(id)
);
