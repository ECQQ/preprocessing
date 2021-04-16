CREATE TABLE personal_need_pair
(
	id integer PRIMARY KEY,
	personal_need_id integer references personal_need(id),
	word_1 varchar(64),
	word_2 varchar(64)
);
