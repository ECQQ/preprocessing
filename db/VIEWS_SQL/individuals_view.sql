CREATE VIEW individuals_view AS
SELECT 
        individuals.id
        age,
        level as education,
        age_range, 
        individuals.date,
        regions.iso as region_iso, 
        regions.name as region_name, 
        regions.numero as region_number, 
        comunas.name as comuna_name

FROM
        comunas,
        regions,
        individuals
WHERE 
        individuals.comuna_id = comunas.id and 
        comunas.region_iso = regions.iso 
