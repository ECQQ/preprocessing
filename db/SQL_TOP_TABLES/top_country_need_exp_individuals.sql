CREATE TABLE top_country_need_exp_individuals (
    word_1 varchar(64),
    macro varchar(128)
);


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

SELECT * FROM getTop10CountryNeedExpByMacroIndividuals();