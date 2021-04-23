CREATE TABLE top_country_need_role_dialogues (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE TABLE top_country_need_exp_dialogues (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE TABLE top_personal_need_dialogues (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE TABLE top_emotions_dialogues (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE OR REPLACE FUNCTION getTop10EmotionsByMacroDialogues() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_emotions_dialogues
    LOOP
        INSERT INTO top_emotions_dialogues
        SELECT emotions_pairs.word_1, emotions.name as macro
        FROM emotions_pairs, emotions
        WHERE 
        emotions_pairs.emotion_id = emotions.id AND
        emotions.name = temprow.name AND
        emotions.diag_id IS NOT null
        GROUP BY emotions_pairs.word_1, emotions.name
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getTop10PersonalNeedByMacroDialogues() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_personal_needs_dialogues
    LOOP
        INSERT INTO top_personal_need_dialogues
        SELECT personal_needs_pairs.word_1, personal_needs.macro
        FROM personal_needs_pairs, personal_needs
        WHERE 
        personal_needs_pairs.personal_need_id = personal_needs.id AND
        personal_needs.macro = temprow.macro AND
        personal_needs.diag_id IS NOT null
        GROUP BY personal_needs_pairs.word_1, personal_needs.macro
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION getTop10CountryNeedRoleByMacroDialogues() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_country_needs_dialogues
    LOOP
        INSERT INTO top_country_need_role_dialogues
        SELECT country_needs_role_pairs.word_1, country_needs.name as macro
        FROM country_needs_role_pairs, country_needs
        WHERE 
        country_needs_role_pairs.country_need_id = country_needs.id AND
        country_needs.name = temprow.macro AND
        country_needs.diag_id IS NOT null
        GROUP BY country_needs_role_pairs.word_1, country_needs.name
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getTop10CountryNeedExpByMacroDialogues() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_country_needs_dialogues
    LOOP
        INSERT INTO top_country_need_exp_dialogues
        SELECT country_needs_exp_pairs.word_1, country_needs.name as macro
        FROM country_needs_exp_pairs, country_needs
        WHERE 
        country_needs_exp_pairs.country_need_id = country_needs.id AND
        country_needs.name = temprow.macro AND
        country_needs.diag_id IS NOT null
        GROUP BY country_needs_exp_pairs.word_1, country_needs.name
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM getTop10CountryNeedRoleByMacroDialogues();
SELECT * FROM getTop10CountryNeedExpByMacroDialogues();
SELECT * FROM getTop10PersonalNeedByMacroDialogues();
SELECT * FROM getTop10EmotionsByMacroDialogues();