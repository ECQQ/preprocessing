CREATE TABLE person
(
	id integer PRIMARY KEY,
	diag_id varchar(32) REFERENCES dialogue(id),
	age integer,
	sex varchar(2),
	level varchar(64),
	age_range varchar(5),	
	comuna integer REFERENCES comuna(id)
);
