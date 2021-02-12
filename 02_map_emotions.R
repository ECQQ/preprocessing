



library(readr)
library(clustringr)
library(stringr)
library(tidyverse)



removeAccents<-function(x)
{
  a <- c('À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', 'Ø', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'ß', 'à', 'á', 'â', 'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í', 'î', 'ï', 'ñ', 'ò', 'ó', 'ô', 'õ', 'ö', 'ø', 'ù', 'ú', 'û', 'ü', 'ý', 'ÿ', 'Ā', 'ā', 'Ă', 'ă', 'Ą', 'ą', 'Ć', 'ć', 'Ĉ', 'ĉ', 'Ċ', 'ċ', 'Č', 'č', 'Ď', 'ď', 'Đ', 'đ', 'Ē', 'ē', 'Ĕ', 'ĕ', 'Ė', 'ė', 'Ę', 'ę', 'Ě', 'ě', 'Ĝ', 'ĝ', 'Ğ', 'ğ', 'Ġ', 'ġ', 'Ģ', 'ģ', 'Ĥ', 'ĥ', 'Ħ', 'ħ', 'Ĩ', 'ĩ', 'Ī', 'ī', 'Ĭ', 'ĭ', 'Į', 'į', 'İ', 'ı', 'Ĳ', 'ĳ', 'Ĵ', 'ĵ', 'Ķ', 'ķ', 'Ĺ', 'ĺ', 'Ļ', 'ļ', 'Ľ', 'ľ', 'Ŀ', 'ŀ', 'Ł', 'ł', 'Ń', 'ń', 'Ņ', 'ņ', 'Ň', 'ň', 'ŉ', 'Ō', 'ō', 'Ŏ', 'ŏ', 'Ő', 'ő', 'Œ', 'œ', 'Ŕ', 'ŕ', 'Ŗ', 'ŗ', 'Ř', 'ř', 'Ś', 'ś', 'Ŝ', 'ŝ', 'Ş', 'ş', 'Š', 'š', 'Ţ', 'ţ', 'Ť', 'ť', 'Ŧ', 'ŧ', 'Ũ', 'ũ', 'Ū', 'ū', 'Ŭ', 'ŭ', 'Ů', 'ů', 'Ű', 'ű', 'Ų', 'ų', 'Ŵ', 'ŵ', 'Ŷ', 'ŷ', 'Ÿ', 'Ź', 'ź', 'Ż', 'ż', 'Ž', 'ž', 'ſ', 'ƒ', 'Ơ', 'ơ', 'Ư', 'ư', 'Ǎ', 'ǎ', 'Ǐ', 'ǐ', 'Ǒ', 'ǒ', 'Ǔ', 'ǔ', 'Ǖ', 'ǖ', 'Ǘ', 'ǘ', 'Ǚ', 'ǚ', 'Ǜ', 'ǜ', 'Ǻ', 'ǻ', 'Ǽ', 'ǽ', 'Ǿ', 'ǿ');
  b <- c('A', 'A', 'A', 'A', 'A', 'A', 'AE', 'C', 'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I', 'D', 'N', 'O', 'O', 'O', 'O', 'O', 'O', 'U', 'U', 'U', 'U', 'Y', 's', 'a', 'a', 'a', 'a', 'a', 'a', 'ae', 'c', 'e', 'e', 'e', 'e', 'i', 'i', 'i', 'i', 'n', 'o', 'o', 'o', 'o', 'o', 'o', 'u', 'u', 'u', 'u', 'y', 'y', 'A', 'a', 'A', 'a', 'A', 'a', 'C', 'c', 'C', 'c', 'C', 'c', 'C', 'c', 'D', 'd', 'D', 'd', 'E', 'e', 'E', 'e', 'E', 'e', 'E', 'e', 'E', 'e', 'G', 'g', 'G', 'g', 'G', 'g', 'G', 'g', 'H', 'h', 'H', 'h', 'I', 'i', 'I', 'i', 'I', 'i', 'I', 'i', 'I', 'i', 'IJ', 'ij', 'J', 'j', 'K', 'k', 'L', 'l', 'L', 'l', 'L', 'l', 'L', 'l', 'l', 'l', 'N', 'n', 'N', 'n', 'N', 'n', 'n', 'O', 'o', 'O', 'o', 'O', 'o', 'OE', 'oe', 'R', 'r', 'R', 'r', 'R', 'r', 'S', 's', 'S', 's', 'S', 's', 'S', 's', 'T', 't', 'T', 't', 'T', 't', 'U', 'u', 'U', 'u', 'U', 'u', 'U', 'u', 'U', 'u', 'U', 'u', 'W', 'w', 'Y', 'y', 'Y', 'Z', 'z', 'Z', 'z', 'Z', 'z', 's', 'f', 'O', 'o', 'U', 'u', 'A', 'a', 'I', 'i', 'O', 'o', 'U', 'u', 'U', 'u', 'U', 'u', 'U', 'u', 'U', 'u', 'A', 'a', 'AE', 'ae', 'O', 'o');
  for(i in 1:length(a))
  {
    x<-gsub(x = x,pattern = a[i],replacement = b[i])
  }
  return(x)
}



emo_per_user_v2 <- read_csv("emo_per_user_v2.csv")

CompanyName = tolower(unique(emo_per_user$emotion)) # otherwise case matters too much
# Calculate a string distance matrix; LCS is just one option
#rm(sdm)

cluster_words <- cluster_strings(CompanyName, clean = F, method = "osa", max_dist = 1,
                                 algo = "cc")

df_clusters <- cluster_words$df_clusters


writexl::write_xlsx(df_clusters, "df_clusters.xlsx")

### reclasifico a mano

library(readxl)

df_clusters <- read_excel("df_clusters.xlsx")

df_clusters_join <- df_clusters %>%
  select(emotion_verified)

df_clusters_join <- df_clusters_join %>%
  distinct()

writexl::write_xlsx(df_clusters_join, "df_clusters_join.xlsx")

# reclasifico a mano

library(readxl)

df_clusters_join <- read_excel("df_clusters_join.xlsx")


df_clusters <- df_clusters %>%
  left_join(df_clusters_join, by="emotion_verified")

emo_per_user <- emo_per_user %>%
  left_join(df_clusters, by=c("emotion"="node"))

df_count <- emo_per_user_v2 %>%
  count(emotion_verified2) # hacemos un conteo


emo_per_user_v2 <- emo_per_user_v2 %>%
  left_join(df_clusters, by=c("emotion"="node"))

# replaces comunas 

emo_per_user_v2$comuna[emo_per_user_v2$comuna=="las palmas"] <- "llay llay"
emo_per_user_v2$comuna[emo_per_user_v2$comuna=="antartida"] <- "cabo de hornos"

###################


matching_shp_bbdd_dialogos <- read_csv("matching_shp_bbdd_dialogos.csv", 
                                       locale = locale(encoding = "latin1"))

df2 <- matching_shp_bbdd_dialogos %>%
  select(Comuna, code_comuna)

#head(df2)
#df2 <- as.data.frame(df2)
#df2 <- df2 %>%
# select(-geometry)

df2$Comuna <- tolower(df2$Comuna)
df2$Comuna <- removeAccents(df2$Comuna)


dff <- df %>%
  left_join(df2, by=c("comunas"="Comuna"))

dff <- dff %>%
  rename(comuna=comunas)


emo_per_user_v2 <- emo_per_user_v2 %>%
  left_join(dff, by="comuna")

names(emo_per_user_v2)

emo_per_user_v2 <- emo_per_user_v2 %>%
  select(id_file, id_user, comuna, code_comuna, region, group, age, age_range,
         education, sex, priority, emotion,emotion_verified, emotion_verified2)

write_csv(emo_per_user_v2, "emo_per_user_v3.csv")