CREATE TABLE top_10_contributions_individuals AS

SELECT c.macro 
FROM contributions as c
WHERE ind_id IS NOT null
GROUP BY c.macro 
ORDER BY COUNT(*) DESC LIMIT 10;