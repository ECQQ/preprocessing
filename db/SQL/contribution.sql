CREATE TABLE contribution
(
	id integer PRIMARY KEY,
	diag_id varchar(32) REFERENCES dialogue(id),
	ind_id varchar(32) REFERENCES individual(id),
	text varchar(256),
	tokens json,
	macro varchar(64),
	is_online integer	
);
