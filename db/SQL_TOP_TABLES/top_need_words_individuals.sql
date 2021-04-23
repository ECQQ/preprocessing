CREATE TABLE top_country_need_role_individuals (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE TABLE top_country_need_exp_individuals (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE TABLE top_personal_need_individuals (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE TABLE top_emotions_individuals (
    word_1 varchar(64),
    macro varchar(128)
);

CREATE OR REPLACE FUNCTION getTop10EmotionsByMacroIndividuals() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_emotions_individuals
    LOOP
        INSERT INTO top_emotions_individuals
        SELECT emotions_pairs.word_1, emotions.name as macro
        FROM emotions_pairs, emotions
        WHERE 
        emotions_pairs.emotion_id = emotions.id AND
        emotions.name = temprow.name AND
        emotions.ind_id IS NOT null
        GROUP BY emotions_pairs.word_1, emotions.name
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getTop10PersonalNeedByMacroIndividuals() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_personal_needs_individuals
    LOOP
        INSERT INTO top_personal_need_individuals
        SELECT personal_needs_pairs.word_1, personal_needs.macro
        FROM personal_needs_pairs, personal_needs
        WHERE 
        personal_needs_pairs.personal_need_id = personal_needs.id AND
        personal_needs.macro = temprow.macro AND
        personal_needs.ind_id IS NOT null
        GROUP BY personal_needs_pairs.word_1, personal_needs.macro
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION getTop10CountryNeedRoleByMacroIndividuals() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_country_needs_individuals
    LOOP
        INSERT INTO top_country_need_role_individuals
        SELECT country_needs_role_pairs.word_1, country_needs.name as macro
        FROM country_needs_role_pairs, country_needs
        WHERE 
        country_needs_role_pairs.country_need_id = country_needs.id AND
        country_needs.name = temprow.macro AND
        country_needs.ind_id IS NOT null
        GROUP BY country_needs_role_pairs.word_1, country_needs.name
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getTop10CountryNeedExpByMacroIndividuals() RETURNS void AS $$
declare
    TEMPROW record;
BEGIN
FOR temprow IN
        SELECT * FROM top_10_country_needs_individuals
    LOOP
        INSERT INTO top_country_need_exp_individuals
        SELECT country_needs_exp_pairs.word_1, country_needs.name as macro
        FROM country_needs_exp_pairs, country_needs
        WHERE 
        country_needs_exp_pairs.country_need_id = country_needs.id AND
        country_needs.name = temprow.macro AND
        country_needs.ind_id IS NOT null
        GROUP BY country_needs_exp_pairs.word_1, country_needs.name
        ORDER BY COUNT(*) DESC LIMIT 10;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM getTop10CountryNeedRoleByMacroIndividuals();
SELECT * FROM getTop10CountryNeedExpByMacroIndividuals();
SELECT * FROM getTop10PersonalNeedByMacroIndividuals();
SELECT * FROM getTop10EmotionsByMacroIndividuals();