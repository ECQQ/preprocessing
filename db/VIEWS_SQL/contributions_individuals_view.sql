CREATE VIEW contributions_individuals_view AS
SELECT 
	d.id, 
	d.date,
	d.online,
	-- d.valid,
	c.macro,
	regions.iso as region_iso, 
	regions.name as region_name, 
	regions.numero as region_number,
	regions.orden as region_order,  
	comunas.name as comuna_name
FROM 
	comunas,
	regions,
	contributions as c, 
	individuals as d
WHERE 
	d.comuna_id = comunas.id and 
	comunas.region_iso = regions.iso and
	c.ind_id = d.id