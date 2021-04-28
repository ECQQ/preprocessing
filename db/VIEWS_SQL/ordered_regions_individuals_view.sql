CREATE VIEW ordered_regions_individuals_view AS

SELECT region_name, COUNT(*) FROM (SELECT 
        individuals.id
        age,
        level as education,
        age_range, 
        individuals.date,
        regions.iso as region_iso, 
        regions.name as region_name, 
        regions.numero as region_number,
	regions.orden as region_order, 
        comunas.name as comuna_name

FROM
        comunas,
        regions,
        individuals
WHERE 
        individuals.comuna_id = comunas.id and 
        comunas.region_iso = regions.iso) as foo 
GROUP BY region_name, region_order  
ORDER BY region_order