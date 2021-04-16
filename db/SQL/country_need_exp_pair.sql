CREATE TABLE country_need_exp_pair
(
	id integer PRIMARY KEY,
	country_need_id integer references country_need(id),
	word_1 varchar(64),
	word_2 varchar(64)
);
