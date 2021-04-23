CREATE TABLE top_50_country_needs_dialogues AS

SELECT cn.name as macro
FROM country_needs as cn
WHERE diag_id IS NOT null
GROUP BY cn.name 
ORDER BY COUNT(*) DESC LIMIT 50;