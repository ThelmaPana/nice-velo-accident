#--------------------------------------------------------------------------#
# Project: nice-velo-accident
# Script purpose: Nettoyer les données d’accidentologie à Nice
# Date: 18/03/2026
# Author: Thelma Panaïotis
#
# Ce script :
# - Harmonise les types de variables
# - Factorise les variables catégorielles avec gestion des -1
# - Filtre les accidents vélo et les usagers cyclistes uniquement
# - Calcule les configurations d'accidents
# - Exporte les données nettoyées pour l'analyse
#
#--------------------------------------------------------------------------#

library(tidyverse)
library(here)


## Chargement des données brutes ----
#--------------------------------------------------------------------------#
caracteristiques <- readRDS(here("data", "02.caracteristiques.rds"))
lieux            <- readRDS(here("data", "02.lieux.rds"))
vehicules        <- readRDS(here("data", "02.vehicules.rds"))
usagers          <- readRDS(here("data", "02.usagers.rds"))


## Labels des variables ----
#--------------------------------------------------------------------------#

# Type de collision
col_labels <- c(
  "1" = "Deux véhicules - frontale",
  "2" = "Deux véhicules - par l'arrière",
  "3" = "Deux véhicules - par le côté",
  "4" = "Trois véhicules et + - en chaîne",
  "5" = "Trois véhicules et + - collisions multiples",
  "6" = "Autre collision",
  "7" = "Sans collision"
)

# Gravité
grav_labels <- c(
  "1" = "Indemne",
  "4" = "Blessé léger",
  "3" = "Blessé hospitalisé",
  "2" = "Tué"
)

# Luminosité
lum_labels <- c(
  "1" = "Plein jour",
  "2" = "Crépuscule ou aube",
  "3" = "Nuit sans éclairage public",
  "4" = "Nuit, éclairage public éteint",
  "5" = "Nuit, éclairage public allumé"
)

# Conditions atmosphériques
atm_labels <- c(
  "1" = "Normale",
  "2" = "Pluie légère",
  "3" = "Pluie forte",
  "4" = "Neige/grêle",
  "5" = "Brouillard/fumée",
  "6" = "Vent fort/tempête",
  "7" = "Temps éblouissant",
  "8" = "Temps couvert",
  "9" = "Autre"
)


## Harmonisation des types ----
#--------------------------------------------------------------------------#
vehicules <- vehicules %>%
  mutate(
    Num_Acc = as.character(Num_Acc),
    catv    = as.integer(catv)
  )

usagers <- usagers %>%
  mutate(
    Num_Acc  = as.character(Num_Acc),
    grav     = as.integer(grav),
    catu     = as.integer(catu),
    sexe     = as.integer(sexe),
    an_nais  = as.integer(an_nais)
  )

lieux <- lieux %>%
  mutate(Num_Acc = as.character(Num_Acc))

caracteristiques <- caracteristiques %>%
  mutate(
    Num_Acc = as.character(Num_Acc),
    lum = as.integer(lum),
    atm = as.integer(atm),
    col = as.integer(col)
  )

## Factorisation avec gestion des -1 (i.e. NA) ----
#--------------------------------------------------------------------------#

caracteristiques <- caracteristiques %>%
  mutate(
    lum_label = if_else(
      lum == -1,
      "Non renseigné",
      recode(as.character(lum), !!!lum_labels)
    ),
    lum_label = if (any(caracteristiques$lum == -1, na.rm = TRUE)) {
      factor(lum_label, levels = c(unname(lum_labels), "Non renseigné"))
    } else {
      factor(lum_label, levels = unname(lum_labels))
    },

    atm_label = if_else(
      atm == -1,
      "Non renseigné",
      recode(as.character(atm), !!!atm_labels)
    ),
    atm_label = if (any(caracteristiques$atm == -1, na.rm = TRUE)) {
      factor(atm_label, levels = c(unname(atm_labels), "Non renseigné"))
    } else {
      factor(atm_label, levels = unname(atm_labels))
    },

    col_label = if_else(
      col == -1,
      "Non renseigné",
      recode(as.character(col), !!!col_labels)
    ),
    col_label = if (any(caracteristiques$col == -1, na.rm = TRUE)) {
      factor(col_label, levels = c(unname(col_labels), "Non renseigné"))
    } else {
      factor(col_label, levels = unname(col_labels))
    }
  )

usagers <- usagers %>%
  mutate(
    grav_label = if_else(
      grav == -1,
      "Non renseigné",
      recode(as.character(grav), !!!grav_labels)
    ),
    grav_label = if (any(usagers$grav == -1, na.rm = TRUE)) {
      factor(grav_label, levels = c(unname(grav_labels), "Non renseigné"))
    } else {
      factor(grav_label, levels = unname(grav_labels))
    },

    sexe_label = case_when(
      sexe == -1 ~ "Non renseigné",
      sexe == 1 ~ "Homme",
      sexe == 2 ~ "Femme",
      TRUE ~ "Autre"
    ),
    sexe_label = {
      has_missing <- any(usagers$sexe == -1, na.rm = TRUE)
      has_other <- any(!usagers$sexe %in% c(-1, 1, 2), na.rm = TRUE)

      levels_vec <- c("Homme", "Femme")
      if (has_missing) levels_vec <- c(levels_vec, "Non renseigné")
      if (has_other) levels_vec <- c(levels_vec, "Autre")

      factor(sexe_label, levels = levels_vec)
    }
  )


## Filtrage accidents vélo ----
#--------------------------------------------------------------------------#

# Identification des accidents impliquant au moins un vélo
# catv = 1 : bicyclette classique
# catv = 80 : vélo à assistance électrique (VAE)
acc_velo <- vehicules %>%
  filter(catv %in% c(1, 80)) %>%
  pull(Num_Acc) %>%
  unique()

cat("Nombre d'accidents impliquant un vélo:", length(acc_velo), "\n")

# Filtrage des tables
caract_velo    <- caracteristiques %>% filter(Num_Acc %in% acc_velo)
lieux_velo     <- lieux            %>% filter(Num_Acc %in% acc_velo)
vehicules_velo <- vehicules        %>% filter(Num_Acc %in% acc_velo)

# Filtrage usagers : uniquement les cyclistes (personnes sur un vélo)
velos_num <- vehicules_velo %>%
  filter(catv %in% c(1, 80)) %>%
  select(Num_Acc, num_veh)

usagers_velo <- usagers %>%
  #filter(Num_Acc %in% acc_velo) %>%
  inner_join(velos_num, by = c("Num_Acc", "num_veh")) |>
  filter(catu %in% c(1, 2)) #1=conducteur, 2=passager

cat("Nombre de cyclistes (usagers sur un vélo):", nrow(usagers_velo), "\n")


## Configurations d'accidents ----
#--------------------------------------------------------------------------#

# Identifier les accidents avec piétons
acc_avec_pieton <- usagers %>%
  filter(catu == 3) %>%  # 3 = piéton
  pull(Num_Acc) %>%
  unique()

# Pour chaque accident, compter les types de véhicules
config_accidents <- vehicules_velo %>%
  mutate(
    type_vehicule = case_when(
      catv %in% c(1, 80) ~ "velo",
      catv == 7 ~ "voiture",
      catv %in% c(10, 13, 14, 15, 16, 17) ~ "pl",
      catv %in% c(2, 30, 31, 32, 33, 34) ~ "deux_roues_motorise",
      catv == 50 ~ "edpm_motorise",
      catv == 60 ~ "edpm_non_motorise",
      TRUE ~ "autre"
    )
  ) %>%
  filter(!is.na(type_vehicule)) %>%
  group_by(Num_Acc) %>%
  summarise(
    n_velo = sum(type_vehicule == "velo"),
    n_voiture = sum(type_vehicule == "voiture"),
    n_pl = sum(type_vehicule == "pl"),
    n_2rm = sum(type_vehicule == "deux_roues_motorise"),
    n_edpm_m = sum(type_vehicule == "edpm_motorise"),
    n_edpm_nm = sum(type_vehicule == "edpm_non_motorise"),
    n_autre = sum(type_vehicule == "autre"),
    n_total = n(),
    .groups = "drop"
  ) %>%
  mutate(
    avec_pieton = Num_Acc %in% acc_avec_pieton,
    configuration = case_when(
      avec_pieton & n_velo >= 1 ~ "Vélo vs piéton",
      n_total == 1 & n_velo == 1 ~ "Vélo seul",
      n_velo >= 2 & n_total == n_velo ~ "Vélo vs vélo",
      n_velo >= 1 & n_voiture >= 1 & n_total == (n_velo + n_voiture) ~ "Vélo vs voiture",
      n_velo >= 1 & n_pl >= 1 ~ "Vélo vs poids lourd",
      n_velo >= 1 & n_2rm >= 1 & n_total == (n_velo + n_2rm) ~ "Vélo vs 2RM",
      n_velo >= 1 & (n_edpm_m >= 1 | n_edpm_nm >= 1) ~ "Vélo vs EDPM",
      TRUE ~ "Autre configuration"
    )
  )

cat("Configurations calculées pour", nrow(config_accidents), "accidents\n")


## Export des données nettoyées ----
#--------------------------------------------------------------------------#

saveRDS(caract_velo,      here("data", "03.caracteristiques_velo_clean.rds"))
saveRDS(usagers_velo,     here("data", "03.usagers_velo_clean.rds"))
saveRDS(vehicules_velo,   here("data", "03.vehicules_velo_clean.rds"))
saveRDS(lieux_velo,       here("data", "03.lieux_velo_clean.rds"))
saveRDS(config_accidents, here("data", "03.config_accidents.rds"))

# Garder aussi les tables complètes factorisées pour analyses futures
saveRDS(caracteristiques, here("data", "03.caracteristiques_clean.rds"))
saveRDS(usagers,          here("data", "03.usagers_clean.rds"))
saveRDS(vehicules,        here("data", "03.vehicules_clean.rds"))

cat("\n✓ Données nettoyées exportées dans /data/\n")
