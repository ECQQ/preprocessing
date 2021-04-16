CREATE TABLE emotion_pair
(
	id integer PRIMARY KEY,
	emotion_id integer references emotion(id),
	word_1 varchar(64),
	word_2 varchar(64)
);
