CREATE VIEW country_needs_individuals_view as 
SELECT 
	d.id, 
	d.date, 
	-- d.valid,
	d.online,
	cn.macro,
	cn.name,
	cn.role,
	cn.actor,
	cn.priority,
	regions.iso as region_iso, 
	regions.name as region_name, 
	regions.numero as region_number, 
	comunas.name as comuna_name
FROM 
	comunas,
	regions,
	country_needs as cn, 
	individuals as d
WHERE
	d.comuna_id = comunas.id and 
	comunas.region_iso = regions.iso and
	cn.ind_id = d.id
