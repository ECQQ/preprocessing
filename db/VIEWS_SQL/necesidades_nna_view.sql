CREATE VIEW necesidades_nna_view AS
SELECT DISTINCT 
    id_mesa,
    cantidad_participantes,
    rango_edades_id,
    region_name,
    region_iso
FROM 
    necesidades_nna;