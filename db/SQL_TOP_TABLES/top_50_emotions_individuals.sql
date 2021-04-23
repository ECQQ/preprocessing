CREATE TABLE top_50_emotions_individuals AS

SELECT e.name 
FROM emotions as e
WHERE ind_id IS NOT null
GROUP BY name 
ORDER BY COUNT(*) DESC LIMIT 50;