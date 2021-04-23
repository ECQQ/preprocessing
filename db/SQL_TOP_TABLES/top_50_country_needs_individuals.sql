CREATE TABLE top_50_country_needs_individuals AS

SELECT cn.name as macro
FROM country_needs as cn
WHERE ind_id IS NOT null
GROUP BY cn.name 
ORDER BY COUNT(*) DESC LIMIT 50;