CREATE TABLE top_50_emotions_dialogues AS

SELECT e.name 
FROM emotions as e
WHERE diag_id IS NOT null
GROUP BY name 
ORDER BY COUNT(*) DESC LIMIT 50;


