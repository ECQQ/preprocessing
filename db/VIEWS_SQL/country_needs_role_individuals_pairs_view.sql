CREATE VIEW country_needs_role_individuals_pairs_view AS
SELECT 
	d.id, 
	d.date, 
	d.online,
	n.macro,
	n.actor,
	regions.iso as region_iso, 
	regions.name as region_name, 
	regions.numero as region_number, 
	comunas.name as comuna_name,
	w.word_1,
	w.word_2
FROM
	comunas,
	regions,
	country_needs as n, 
	individuals as d,
	country_needs_role_pairs as w
WHERE 
	d.comuna_id = comunas.id and 
	comunas.region_iso = regions.iso and
	n.ind_id = d.id and
	w.country_need_id = n.id and
	w.word_1 IN 
		(SELECT tnw.word_1 from top_country_need_role_individuals as tnw where tnw.macro = n.macro) AND
	w.word_2 IN (SELECT tnw.word_1 from top_country_need_role_individuals as tnw where tnw.macro = n.macro)
