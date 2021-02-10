
#####################

library(tidyverse)

library(readxl)

BBDD_Diaogos_final <- read_excel("input/Dialogo/BBDD_Diaogos_final.xlsx")


BBDD_Diaogos_final_coms <- BBDD_Diaogos_final %>%
  select(Comuna)


BBDD_Diaogos_final_coms <- BBDD_Diaogos_final_coms %>%
  distinct()

BBDD_Diaogos_final_coms$Comuna <- toupper(BBDD_Diaogos_final_coms$Comuna) 




library(sf)

#shp_comunas <- st_read("input/r_comunas_all.shp")

#shp_comunas <- as.data.frame(shp_comunas)

#shp_comunas <- shp_comunas %>%
 # select(COMUNA,NOM_COMUNA)

library(readr)

comunas_cod_names_district <- read_csv("input/comunas_cod_names_district.csv")


comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='COYHAIQUE']<-"COIHAIQUE"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='LOS ÁNGELES']<-"LOS ANGELES"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='AYSÉN']<-"AISÉN"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=="O'HIGGINS"]<-"OHIGGINS"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='OLLAGÜE']<-"OLLAGUE"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='PAIGUANO']<-"PAIHUANO"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='LLAILLAY']<-"LLAY LLAY"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='TREGUACO']<-"TREHUACO"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='RÁNQUIL']<-"RANQUIL"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='LOS ÁLAMOS']<-"LOS ALAMOS"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='TILTIL']<-"TIL TIL"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='TIRÚA']<-"TIRUA"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='REQUÍNOA']<-"REQUINOA"
comunas_cod_names_district$NOM_COMUNA[comunas_cod_names_district$NOM_COMUNA=='HUALAIHUÉ']<-"HUALAIHUE"


BBDD_Diaogos_final_match <- BBDD_Diaogos_final_coms %>%
  left_join(comunas_cod_names_district, by=c("Comuna"="NOM_COMUNA"))


BBDD_Diaogos_final_match %>% filter(is.na(COMUNA))

BBDD_Diaogos_final$Comuna <- toupper(BBDD_Diaogos_final$Comuna)


# con esto agregas el código territorial a la base de dialogos final que 
# te permite hacer el join con los shapes 
BBDD_Diaogos_final <- BBDD_Diaogos_final %>%
  left_join(BBDD_Diaogos_final_match %>% select(Comuna, COMUNA), by=("Comuna"))


# así deberías hacer cambios y luego el join con los shapes 


shp_comunas$COMUNA <- as.double(shp_comunas$COMUNA)


BBDD_Diaogos_final_shape <- BBDD_Diaogos_final %>% # filter(!is.na(Comuna)) %>%
  left_join(shp_comunas, by="COMUNA")
  

# entonces dejo .csv: 1) bbdd_dialogos_final_match que tiene los codigos asociados para añadir los códigos a la base de dialogos

BBDD_Diaogos_final_match <- BBDD_Diaogos_final_match %>% rename(code_comuna=COMUNA) %>%
  select(Comuna, code_comuna)

write.csv(BBDD_Diaogos_final_match, "output/matching_shp_bbdd_dialogos.csv")
