CREATE TABLE top_50_contributions_individuals AS

SELECT c.macro 
FROM contributions as c
WHERE diag_id IS NOT null and 
c.macro <> 'respuesta sin completar' and 
c.macro <> '.' and
c.macro <> '-' AND c.macro <> '?' 
GROUP BY c.macro 
ORDER BY COUNT(*) DESC LIMIT 50;