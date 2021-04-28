#!/bin/bash

# Crea las tablas de emociones y contribuciones definidas en SQL_TOP_TABLES

puerto=5432
rol=superset
db=ecqq2

create_table=`docker exec -it superset_db psql -h localhost -p $puerto -U $rol -c `
copy_csv=`docker exec -it superset_db psql -h localhost -p $puerto -U $rol -c `

echo "Borrando antigua bd $db"
docker exec -it superset_db  psql -h localhost -p $puerto -U $rol -c "DROP DATABASE $db;"


echo "Creando bd $db"
docker exec -it superset_db psql -h localhost -p $puerto -U $rol -c "CREATE DATABASE $db;"
docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db  -c "ALTER DATABASE $db SET datestyle TO \"ISO, DMY\";"
	
echo "Copiando archivos al contenedor"

docker cp CSV superset_db:/data
docker cp SQL_INSERT superset_db:/data

tables=(
	regions 
	comunas 
	dialogues
	individuals
	persons
	persons_dialogues
	emotions
	contributions
	country_needs
	personal_needs
	emotions_pairs
	country_needs_exp_pairs
	country_needs_role_pairs
	personal_needs_pairs
)


views=(
	contributions_dialogues_view
	contributions_individuals_view
	country_needs_dialogue_view
	country_needs_exp_dialogues_pairs_view
	country_needs_exp_individuals_pairs_view
	country_needs_individuals_view
	country_needs_role_dialogues_pairs_view
	country_needs_role_individuals_pairs_view
	dialogues_view
	emotions_dialogues_pairs_view
	emotions_dialogue_view
	emotions_individuals_pairs_view
	emotions_individuals_view
	individuals_view
	ordered_regions_dialogues_view
	ordered_regions_individuals_view
	ordered_regions_person
	personal_needs_dialogues_pairs_view
	personal_needs_dialogue_view
	personal_needs_individuals_pairs_view
	personal_needs_individuals_view
	persons_view
)

tops=(
	top_10_contributions_dialogues
	top_10_contributions_individuals
	top_10_country_needs_dialogues
	top_10_country_needs_individuals
	top_10_emotions_dialogues
	top_10_emotions_individuals
	top_10_personal_needs_dialogues
	top_10_personal_needs_individuals
	top_50_contributions_dialogues
	top_50_contributions_individuals
	top_50_country_needs_dialogues
	top_50_country_needs_individuals
	top_50_emotions_dialogues
	top_50_emotions_individuals
	top_50_personal_needs_dialogues
	top_50_personal_needs_individuals
	top_country_need_exp_dialogues
	top_country_need_exp_individuals
	top_country_need_role_dialogues
	top_country_need_role_individuals
	top_emotions_dialogues
	top_emotions_individuals
	top_personal_need_dialogues
	top_personal_need_individuals
)

for file in "${tables[@]}"; do
	echo "creando tabla $file"
	table=$(cat SQL_SCHEMA/$file.sql)
	docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -c "$table"
	echo "poblando $file"	
	if [[ "$file" == "contributions" ]]; then
		docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -f /data/SQL_INSERT/contributions.sql
	elif [[ "$file" == "country_needs" ]]; then
		docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -f /data/SQL_INSERT/country_needs.sql	
	elif [[ "$file" == "personal_needs" ]]; then
		docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -f /data/SQL_INSERT/personal_needs.sql	
	elif [[ "$file" == "emotions" ]]; then
		docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -f /data/SQL_INSERT/emotions.sql			
	else
		csv_query="COPY $file FROM '/data/CSV/$file.csv' WITH DELIMITER ',' CSV HEADER;"	
		docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db  -c "$csv_query"	
	fi
done

for file in "${tops[@]}"; do
	echo "creando tabla $file"
	top=$(cat SQL_TOP_TABLES/$file.sql)
	docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -c "$top"		
done

for file in "${views[@]}"; do
	echo "creando vista $file"
	view=$(cat VIEWS_SQL/$file.sql)
	docker exec -it superset_db psql -h localhost -p $puerto -U $rol $db -c "$view"		
done

