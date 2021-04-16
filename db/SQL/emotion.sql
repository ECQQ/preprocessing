CREATE TABLE emotion
(
	id integer PRIMARY KEY,
	diag_id varchar(32) REFERENCES dialogue(id),
	ind_id varchar(32) REFERENCES individual(id),
	name varchar(32),
	name_tokens json,
	macro varchar(32),
	exp varchar(512),
	exp_tokens json,
	is_online integer
);
