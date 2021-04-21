CREATE TABLE contributions
(
	id integer PRIMARY KEY,
	diag_id varchar(32) REFERENCES dialogues(id),
	ind_id varchar(32) REFERENCES individuals(id),
	text varchar(256),
	tokens json,
	macro varchar(64),
	is_online integer	
);
