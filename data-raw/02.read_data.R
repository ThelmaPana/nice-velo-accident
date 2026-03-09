#--------------------------------------------------------------------------#
# Project: nice-velo-accident
# Script purpose: Télécharger et normaliser les données d'accidents par commune
# Date: 09/03/2026
# Author: Thelma Panaïotis
#
# Paramètres prévus pour le futur package R :
#   - code_commune : code INSEE 5 caractères (ex: "06088" pour Nice)
#   - annees       : vecteur d'années (ex: 2005:2024)
#
# Deux formats coexistent dans les données BAAC :
#   - Ancien format (<= 2018) : dep encodé ×10 (ex: 590=Nord, 60=Alpes-Maritimes),
#                               com dans le dep, an 2 chiffres,
#                               hrmn HHMM entier, coordonnées Lambert
#   - Nouveau format (>= 2019) : dep entier (6), com 5 chiffres (6088),
#                                an 4 chiffres, hrmn "HH:MM"
#
# Stratégie de lecture :
#   - Priorité à l'API Tabular (filtrage côté serveur, efficace)
#   - Fallback sur lecture du CSV brut si la ressource n'est pas indexée
#     (cas de certaines années : 2005, 2010-2012, potentiellement d'autres)
#
# Attention : l'API Tabular parse incorrectement hrmn en date pour 2019-2023
#             (bug connu, valeurs mises à NA pour ces années).
#--------------------------------------------------------------------------#

library(tidyverse)
library(httr2)
library(here)


## Configuration ----
#--------------------------------------------------------------------------#
# Paramètres futurs du package
code_commune <- "06088"   # Code INSEE de Nice
annees       <- 2005:2024

base_url <- "https://tabular-api.data.gouv.fr/api/resources"


## Catalogue ----
#--------------------------------------------------------------------------#
catalog <- read_csv(here("data-raw", "01.catalog.csv"), show_col_types = FALSE) %>%
  filter(annee %in% annees)


## Fonctions utilitaires ----
#--------------------------------------------------------------------------#

# Décompose un code INSEE commune pour les deux formats de filtre
# code : string 5 chars, ex "06088"
parse_commune_code <- function(code) {
  dep_int <- as.integer(substr(code, 1, 2))
  com_int <- as.integer(substr(code, 3, 5))
  list(
    com_new  = code,                         # "06088" → API interprète comme entier 6088
    dep_old  = as.integer(dep_int * 10),     # dep 06 → 60 (encodage ×10 ancien format)
    com_old  = com_int                       # 88
  )
}

# --- Voie 1 : API Tabular ---

# Effectue une requête paginée sur l'API Tabular, retourne un tibble
fetch_paginated <- function(url) {
  all_data <- list()
  next_url <- url
  page     <- 1

  while (!is.null(next_url)) {
    resp   <- request(next_url) %>% req_perform()
    result <- resp_body_json(resp)

    if (length(result$data) > 0) {
      all_data[[page]] <- map_dfr(result$data, ~ as_tibble(compact(.x)))
    }

    next_url <- result$links$`next`
    page     <- page + 1
  }

  bind_rows(all_data)
}

# Récupère les caractéristiques filtrées par commune (API Tabular)
fetch_caracteristiques_api <- function(resource_id, annee, codes) {
  filter_str <- if (annee >= 2019) {
    paste0("?com__exact=", codes$com_new, "&page_size=500")
  } else {
    paste0("?dep__exact=", codes$dep_old, "&com__exact=", codes$com_old, "&page_size=500")
  }
  url <- paste0(base_url, "/", resource_id, "/data/", filter_str)
  fetch_paginated(url)
}

# Récupère une table secondaire filtrée par Num_Acc (API Tabular)
fetch_by_num_acc_api <- function(resource_id, num_acc) {
  if (length(num_acc) == 0) return(tibble())
  num_acc_str <- paste(num_acc, collapse = ",")
  url <- paste0(
    base_url, "/", resource_id, "/data/",
    "?Num_Acc__in=", num_acc_str, "&page_size=500"
  )
  fetch_paginated(url)
}

# --- Voie 2 : CSV brut (fallback) ---

# Lit un CSV BAAC en détectant automatiquement le séparateur parmi , \t ;
# (les fichiers BAAC historiques varient : virgule en 2005/2012, tabulation en 2009...)
# Toutes les colonnes sont lues en character pour éviter les problèmes de typage
read_baac_csv <- function(url) {
  for (delim in c(",", "\t", ";")) {
    df <- read_delim(
      url, delim = delim, show_col_types = FALSE,
      col_types = cols(.default = col_character()), name_repair = "minimal"
    )
    if (ncol(df) > 1) return(df %>% rename_with(str_trim))
  }
  stop("Impossible de lire le CSV : séparateur non reconnu parmi , \\t ;")
}

# Récupère les caractéristiques filtrées par commune (CSV brut)
fetch_caracteristiques_csv <- function(url_csv, annee, codes) {
  df <- read_baac_csv(url_csv)
  if (annee >= 2019) {
    df %>% filter(as.integer(com) == as.integer(codes$com_new))
  } else {
    df %>% filter(
      as.integer(dep) == codes$dep_old,
      as.integer(com) == codes$com_old
    )
  }
}

# Récupère une table secondaire filtrée par Num_Acc (CSV brut)
fetch_by_num_acc_csv <- function(url_csv, num_acc) {
  df <- read_baac_csv(url_csv)
  df %>% filter(as.character(Num_Acc) %in% as.character(num_acc))
}

# --- Normalisation ---

# Normalise les caractéristiques pour homogénéiser les deux formats
# Colonnes en sortie : Num_Acc, jour, mois, an, hrmn, lum, dep, com,
#                      agg, int, atm, col, adr, lat, long
normalize_caracteristiques <- function(df, annee) {
  df <- df %>% select(-any_of("__id"))

  if (annee <= 2018) {
    df <- df %>%
      mutate(
        an   = 2000L + as.integer(an),
        hrmn = sprintf("%04d", as.integer(hrmn)),
        hrmn = paste0(substr(hrmn, 1, 2), ":", substr(hrmn, 3, 4)),
        dep  = as.integer(dep) %/% 10L,          # 60 -> 6, 590 -> 59
        com  = sprintf("%02d%03d", dep, as.integer(com)),  # reconstitue "06088"
        lat  = NA_real_,                          # Lambert non converti -> NA
        long = NA_real_
      ) %>%
      select(-any_of("gps"))

  } else {
    df <- df %>%
      mutate(
        an   = as.integer(an),
        dep  = as.integer(dep),
        com  = sprintf("%05d", as.integer(com)),  # 6088 -> "06088"
        # CSV BAAC >= 2019 utilise la virgule comme séparateur décimal (ex: "43,7119")
        # On normalise en remplaçant la virgule par un point avant as.numeric()
        lat  = as.numeric(gsub(",", ".", as.character(lat))),
        long = as.numeric(gsub(",", ".", as.character(long))),
        # Bug API Tabular : hrmn HHMM entier parsé en date pour 2019-2023 -> NA
        hrmn = if_else(
          str_detect(coalesce(hrmn, ""), "^\\d{4}-\\d{2}-\\d{2}"),
          NA_character_,
          hrmn
        )
      )
  }

  df
}

clean_table <- function(df) df %>% select(-any_of("__id"))


## Téléchargement ----
#--------------------------------------------------------------------------#
codes <- parse_commune_code(code_commune)

all_caract    <- list()
all_lieux     <- list()
all_vehicules <- list()
all_usagers   <- list()

for (yr in annees) {
  cat("\n=== Année", yr, "===\n")

  cat_yr <- catalog %>% filter(annee == yr)
  if (nrow(cat_yr) == 0) { cat("  Pas de données dans le catalogue\n"); next }

  rid_caract    <- cat_yr %>% filter(type == "caracteristiques") %>% pull(resource_id)
  rid_lieux     <- cat_yr %>% filter(type == "lieux")            %>% pull(resource_id)
  rid_vehicules <- cat_yr %>% filter(type == "vehicules")        %>% pull(resource_id)
  rid_usagers   <- cat_yr %>% filter(type == "usagers")          %>% pull(resource_id)

  url_caract    <- cat_yr %>% filter(type == "caracteristiques") %>% pull(url)
  url_lieux     <- cat_yr %>% filter(type == "lieux")            %>% pull(url)
  url_vehicules <- cat_yr %>% filter(type == "vehicules")        %>% pull(url)
  url_usagers   <- cat_yr %>% filter(type == "usagers")          %>% pull(url)

  # Caractéristiques : API Tabular en priorité, CSV en fallback
  result <- tryCatch(
    list(data = fetch_caracteristiques_api(rid_caract, yr, codes), via_csv = FALSE),
    error = function(e) {
      cat("  API Tabular indisponible, lecture du CSV brut\n")
      list(data = fetch_caracteristiques_csv(url_caract, yr, codes), via_csv = TRUE)
    }
  )
  caract  <- result$data
  via_csv <- result$via_csv

  if (nrow(caract) == 0) { cat("  Aucun accident trouvé pour", code_commune, "\n"); next }

  caract  <- normalize_caracteristiques(caract, yr)
  num_acc <- caract$Num_Acc
  n_geo   <- if ("lat" %in% names(caract)) sum(!is.na(caract$lat)) else 0L
  cat(" ", nrow(caract), "accidents", if (via_csv) "(via CSV)" else "(via API)",
      "—", n_geo, "géolocalisés\n")

  # Tables secondaires : même voie que les caractéristiques
  if (via_csv) {
    lieux     <- fetch_by_num_acc_csv(url_lieux,     num_acc) %>% clean_table()
    vehicules <- fetch_by_num_acc_csv(url_vehicules, num_acc) %>% clean_table()
    usagers   <- fetch_by_num_acc_csv(url_usagers,   num_acc) %>% clean_table()
  } else {
    lieux     <- fetch_by_num_acc_api(rid_lieux,     num_acc) %>% clean_table()
    vehicules <- fetch_by_num_acc_api(rid_vehicules, num_acc) %>% clean_table()
    usagers   <- fetch_by_num_acc_api(rid_usagers,   num_acc) %>% clean_table()
  }

  all_caract[[as.character(yr)]]    <- caract
  all_lieux[[as.character(yr)]]     <- lieux
  all_vehicules[[as.character(yr)]] <- vehicules
  all_usagers[[as.character(yr)]]   <- usagers
}


## Assemblage et sauvegarde ----
#--------------------------------------------------------------------------#
# bind_rows gère les colonnes qui diffèrent entre années
# (ex : env1 absent en 2024, vma absent en 2016) -> colonnes manquantes remplies par NA

caracteristiques <- bind_rows(all_caract)
lieux            <- bind_rows(all_lieux)
vehicules        <- bind_rows(all_vehicules)
usagers          <- bind_rows(all_usagers)

saveRDS(caracteristiques, here("data", "caracteristiques.rds"))
saveRDS(lieux,            here("data", "lieux.rds"))
saveRDS(vehicules,        here("data", "vehicules.rds"))
saveRDS(usagers,          here("data", "usagers.rds"))

cat(
  "\nSauvegardé dans data/ :",
  "\n  caracteristiques.rds :", nrow(caracteristiques), "lignes",
  "\n  lieux.rds            :", nrow(lieux), "lignes",
  "\n  vehicules.rds        :", nrow(vehicules), "lignes",
  "\n  usagers.rds          :", nrow(usagers), "lignes",
  "\n"
)
