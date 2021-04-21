CREATE TABLE emotions
(
	id integer PRIMARY KEY,
	diag_id varchar(32) REFERENCES dialogues(id),
	ind_id varchar(32) REFERENCES individuals(id),
	name varchar(32),
	name_tokens json,
	macro varchar(32),
	exp varchar(512),
	exp_tokens json,
	is_online integer
);
