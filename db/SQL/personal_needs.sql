CREATE TABLE personal_need
(
	id integer PRIMARY KEY,
	diag_id varchar(32) REFERENCES dialogue(id),
	ind_id varchar(32) REFERENCES individual(id),
	name varchar(128),
	name_tokens json,
	macro varchar(128),
	exp varchar(512),
	exp_tokens json,
	priority integer,	
	is_online integer	
);
