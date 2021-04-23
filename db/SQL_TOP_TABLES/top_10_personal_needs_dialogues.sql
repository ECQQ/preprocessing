CREATE TABLE top_10_personal_needs_dialogues AS

SELECT pn.name as macro
FROM personal_needs as pn
WHERE diag_id IS NOT null
GROUP BY pn.name 
ORDER BY COUNT(*) DESC LIMIT 10;